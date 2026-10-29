package repository

import (
	"context"
	"fmt"
	"strings"
	"time"

	"tankard_workdir/lib/clickhouse"
)

const (
	EventsRawTable = "tankard_analytics.events_raw"
	ABDailyTable   = "tankard_analytics.ab_daily"
)

type RawEventRow struct {
	EventID      string `json:"event_id"`
	EventTime    string `json:"event_time"`
	UserID       uint64 `json:"user_id"`
	EventType    string `json:"event_type"`
	Surface      string `json:"surface"`
	ProductID    uint64 `json:"product_id"`
	ExperimentID uint64 `json:"experiment_id"`
	StrategyKey  string `json:"strategy_key"`
}

type ABDailyRow struct {
	Day          string `json:"day"`
	Surface      string `json:"surface"`
	ExperimentID uint64 `json:"experiment_id"`
	StrategyKey  string `json:"strategy_key"`
	Impressions  uint64 `json:"impressions"`
	Clicks       uint64 `json:"clicks"`
	Conversions  uint64 `json:"conversions"`
}

type ClickHouseAnalyticsRepository struct {
	ch *clickhouse.Client
}

func NewClickHouseAnalyticsRepository(ch *clickhouse.Client) *ClickHouseAnalyticsRepository {
	return &ClickHouseAnalyticsRepository{ch: ch}
}

// Client возвращает клиент ClickHouse, чтобы сервис мог выполнять DDL-запросы, например при перестроении агрегата.
func (r *ClickHouseAnalyticsRepository) Client() *clickhouse.Client {
	return r.ch
}

// InsertEvents записывает пакет исходных событий. ReplacingMergeTree при слиянии объединяет строки с одинаковыми (event_time, event_id).
func (r *ClickHouseAnalyticsRepository) InsertEvents(ctx context.Context, rows []RawEventRow) error {
	if len(rows) == 0 {
		return nil
	}

	batch := make([]any, len(rows))
	for i := range rows {
		batch[i] = rows[i]
	}

	if err := r.ch.InsertJSONEachRow(ctx, EventsRawTable, batch); err != nil {
		return fmt.Errorf("insert events_raw: %w", err)
	}

	return nil
}

// CountImpressionsByDay считает строки показов за календарный день по московскому времени.
func (r *ClickHouseAnalyticsRepository) CountImpressionsByDay(ctx context.Context, day time.Time) (uint64, error) {
	query := fmt.Sprintf(
		"SELECT count() AS c FROM %s FINAL WHERE event_type = 'impression' AND toDate(event_time) = toDate('%s')",
		EventsRawTable, day.Format("2006-01-02"),
	)

	var rows []struct {
		C uint64 `json:"c"`
	}
	if err := r.ch.Select(ctx, query, &rows); err != nil {
		return 0, fmt.Errorf("count impressions: %w", err)
	}
	if len(rows) == 0 {
		return 0, nil
	}

	return rows[0].C, nil
}

// LatestImpressionExperiment находит эксперимент последнего показа товара пользователю на указанной поверхности в заданном временном интервале. Это позволяет связать клик с экспериментом, даже если клиент не передал его идентификатор.
func (r *ClickHouseAnalyticsRepository) LatestImpressionExperiment(
	ctx context.Context,
	userID, productID int64,
	surface string,
	window time.Duration,
) (uint64, string, bool, error) {
	query := fmt.Sprintf(`
SELECT argMax(experiment_id, event_time) AS experiment_id,
       argMax(strategy_key, event_time) AS strategy_key
FROM %s FINAL
WHERE event_type = 'impression'
  AND user_id = %d
  AND product_id = %d
  AND surface = '%s'
  AND event_time >= now() - INTERVAL %d SECOND`,
		EventsRawTable, userID, productID, escapeString(surface), int64(window.Seconds()),
	)

	var rows []struct {
		ExperimentID uint64 `json:"experiment_id"`
		StrategyKey  string `json:"strategy_key"`
	}
	if err := r.ch.Select(ctx, query, &rows); err != nil {
		return 0, "", false, fmt.Errorf("lookup latest impression: %w", err)
	}
	if len(rows) == 0 {
		return 0, "", false, nil
	}

	return rows[0].ExperimentID, rows[0].StrategyKey, true, nil
}

// RebuildABDaily пересчитывает ab_daily по events_raw. SummingMergeTree суммирует строки с одинаковым ключом сортировки, поэтому повторная вставка удвоила бы счётчики. Для идемпотентного пересчёта агрегат очищается и заново заполняется запросом INSERT ... SELECT по всем событиям.
func (r *ClickHouseAnalyticsRepository) RebuildABDaily(ctx context.Context) error {
	if err := r.ch.Exec(ctx, "TRUNCATE TABLE "+ABDailyTable); err != nil {
		return fmt.Errorf("truncate ab_daily: %w", err)
	}

	query := fmt.Sprintf(`
INSERT INTO %s
SELECT
    toDate(event_time) AS day,
    surface,
    experiment_id,
    strategy_key,
    countIf(event_type = 'impression') AS impressions,
    countIf(event_type = 'click')      AS clicks,
    countIf(event_type = 'purchase')   AS conversions
FROM %s FINAL
WHERE event_type IN ('impression', 'click', 'purchase')
GROUP BY day, surface, experiment_id, strategy_key`,
		ABDailyTable, EventsRawTable,
	)
	if err := r.ch.Exec(ctx, query); err != nil {
		return fmt.Errorf("insert ab_daily: %w", err)
	}

	if err := r.ch.Exec(ctx, "OPTIMIZE TABLE "+ABDailyTable+" FINAL"); err != nil {
		return fmt.Errorf("optimize ab_daily: %w", err)
	}

	return nil
}

// SelectABDaily читает дневной агрегат за указанный диапазон дат и, при необходимости, для заданной поверхности.
func (r *ClickHouseAnalyticsRepository) SelectABDaily(
	ctx context.Context,
	from, to time.Time,
	surface string,
) ([]ABDailyRow, error) {
	query := fmt.Sprintf(`
SELECT day, surface, experiment_id, strategy_key, impressions, clicks, conversions
FROM %s FINAL
WHERE day >= toDate('%s') AND day <= toDate('%s')`,
		ABDailyTable, from.Format("2006-01-02"), to.Format("2006-01-02"),
	)
	if surface != "" {
		query += fmt.Sprintf(" AND surface = '%s'", escapeString(surface))
	}
	query += " ORDER BY day, surface, experiment_id, strategy_key"

	var rows []ABDailyRow
	if err := r.ch.Select(ctx, query, &rows); err != nil {
		return nil, fmt.Errorf("select ab_daily: %w", err)
	}

	return rows, nil
}

// escapeString подготавливает значение для вставки в SQL-литерал в одинарных кавычках. HTTP-интерфейс принимает запрос через URL-параметр и не поддерживает связывание параметров, поэтому все произвольные значения должны проходить через эту функцию.
func escapeString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}
