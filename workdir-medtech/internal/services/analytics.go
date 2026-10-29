package services

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"medtech-practice/lib/clickhouse"
	pgx_driver "medtech-practice/lib/pgx-driver"

	"medtech-practice/internal/storage"
)

const (
	factTable  = "medtech_analytics.fact_appointments"
	statsTable = "medtech_analytics.daily_provider_stats"
)

var moscow = time.FixedZone("MSK", 3*60*60)

// AnalyticsService — пайплайн аналитики: экспорт фактов в ClickHouse,
// пересборка дневного агрегата и отчёты из OLAP.
type AnalyticsService struct {
	pg      *pgx_driver.Postgres
	storage *storage.AnalyticsStorage
	ch      *clickhouse.Client
	log     *slog.Logger
}

func NewAnalyticsService(
	pg *pgx_driver.Postgres,
	st *storage.AnalyticsStorage,
	ch *clickhouse.Client,
	log *slog.Logger,
) *AnalyticsService {
	return &AnalyticsService{pg: pg, storage: st, ch: ch, log: log}
}

// analyticsFact — строка fact_appointments в формате JSONEachRow.
type analyticsFact struct {
	EventID       string  `json:"event_id"`
	EventTime     string  `json:"event_time"`
	ClinicID      uint64  `json:"clinic_id"`
	ProviderID    uint64  `json:"provider_id"`
	PatientID     uint64  `json:"patient_id"`
	Specialty     string  `json:"specialty"`
	Status        string  `json:"status"`
	NoShowScore   float32 `json:"no_show_score"`
	LeadTimeHours float32 `json:"lead_time_hours"`
}

// SyncFacts выгружает appointments из Postgres в ClickHouse fact_appointments.
// event_id стабилен (apt:{id}:{status}:{updated_at}), поэтому повторный запуск
// не раздувает цифры: ReplacingMergeTree схлопывает строки с одинаковым ключом.
func (s *AnalyticsService) SyncFacts(ctx context.Context) (int, error) {
	const op = "services.analytics.SyncFacts"

	facts, err := s.storage.ListAppointmentsFacts(ctx, s.pg)
	if err != nil {
		return 0, fmt.Errorf("%s: list facts: %w", op, err)
	}

	rows := make([]any, 0, len(facts))
	for _, f := range facts {
		rows = append(rows, analyticsFact{
			EventID:       fmt.Sprintf("apt:%d:%s:%d", f.AppointmentID, f.Status, f.UpdatedAt.Unix()),
			EventTime:     f.StartsAt.In(moscow).Format("2006-01-02 15:04:05.000"),
			ClinicID:      uint64(f.ClinicID),
			ProviderID:    uint64(f.ProviderID),
			PatientID:     uint64(f.PatientID),
			Specialty:     f.Specialty,
			Status:        f.Status,
			NoShowScore:   float32(f.NoShowScore),
			LeadTimeHours: float32(f.StartsAt.Sub(f.CreatedAt).Hours()),
		})
	}

	if err := s.ch.InsertJSONEachRow(ctx, factTable, rows); err != nil {
		return 0, fmt.Errorf("%s: insert: %w", op, err)
	}

	if err := s.ch.Exec(ctx, "OPTIMIZE TABLE "+factTable+" FINAL"); err != nil {
		return 0, fmt.Errorf("%s: optimize: %w", op, err)
	}

	return len(rows), nil
}

// BuildDailyStats пересобирает daily_provider_stats из fact_appointments.
// TRUNCATE + INSERT ... SELECT даёт идемпотентность: агрегат считается заново целиком,
// а FINAL убирает неслитые дубли ReplacingMergeTree.
func (s *AnalyticsService) BuildDailyStats(ctx context.Context) error {
	const op = "services.analytics.BuildDailyStats"

	if err := s.ch.Exec(ctx, "TRUNCATE TABLE "+statsTable); err != nil {
		return fmt.Errorf("%s: truncate: %w", op, err)
	}

	query := fmt.Sprintf(`
INSERT INTO %s
SELECT
    toDate(event_time)            AS day,
    clinic_id,
    provider_id,
    specialty,
    count()                       AS total_appts,
    countIf(status = 'completed') AS completed,
    countIf(status = 'cancelled') AS cancelled,
    countIf(status = 'no_show')   AS no_show
FROM %s FINAL
GROUP BY day, clinic_id, provider_id, specialty`, statsTable, factTable)
	if err := s.ch.Exec(ctx, query); err != nil {
		return fmt.Errorf("%s: aggregate: %w", op, err)
	}

	return nil
}

// DayReport — строка отчёта по одному дню.
type DayReport struct {
	Day       string `json:"day"`
	Total     uint64 `json:"total"`
	NoShow    uint64 `json:"no_show"`
	Cancel    uint64 `json:"cancel"`
	Completed uint64 `json:"completed"`
}

// NoShowReport — отчёт cancel/no-show по дням клиники.
type NoShowReport struct {
	ClinicID int64       `json:"clinic_id"`
	Days     []DayReport `json:"days"`
}

// NoShowReport читает дневной агрегат из ClickHouse (не из тяжёлого SQL Postgres).
func (s *AnalyticsService) NoShowReport(
	ctx context.Context,
	clinicID int64,
	from, to time.Time,
) (*NoShowReport, error) {
	const op = "services.analytics.NoShowReport"

	query := fmt.Sprintf(`
SELECT
    toString(day)    AS day,
    sum(total_appts) AS total,
    sum(no_show)     AS no_show,
    sum(cancelled)   AS cancel,
    sum(completed)   AS completed
FROM %s
WHERE clinic_id = %d AND day >= '%s' AND day <= '%s'
GROUP BY day
ORDER BY day`, statsTable, clinicID, from.Format("2006-01-02"), to.Format("2006-01-02"))

	days := make([]DayReport, 0)
	if err := s.ch.Select(ctx, query, &days); err != nil {
		return nil, fmt.Errorf("%s: %w", op, err)
	}

	return &NoShowReport{ClinicID: clinicID, Days: days}, nil
}

// ReconcileResult — результат сверки Postgres (OLTP) и ClickHouse (OLAP) за день.
type ReconcileResult struct {
	Day             string `json:"day"`
	PostgresCount   int64  `json:"postgres_count"`
	ClickHouseCount int64  `json:"clickhouse_count"`
	Diff            int64  `json:"diff"`
	InSync          bool   `json:"in_sync"`
}

// Reconcile сравнивает число appointments за день в Postgres и fact_appointments в ClickHouse.
// Ненулевой diff означает, что экспортёр отстал/упал — нужно догоняющее перезапущение sync.
func (s *AnalyticsService) Reconcile(ctx context.Context, day time.Time) (*ReconcileResult, error) {
	const op = "services.analytics.Reconcile"

	pgCount, err := s.storage.CountAppointmentsByDay(ctx, s.pg, day)
	if err != nil {
		return nil, fmt.Errorf("%s: postgres count: %w", op, err)
	}

	query := fmt.Sprintf(
		"SELECT count() AS cnt FROM %s FINAL WHERE toDate(event_time) = '%s'",
		factTable, day.Format("2006-01-02"),
	)
	var rows []struct {
		Cnt int64 `json:"cnt"`
	}
	if err := s.ch.Select(ctx, query, &rows); err != nil {
		return nil, fmt.Errorf("%s: clickhouse count: %w", op, err)
	}

	var chCount int64
	if len(rows) > 0 {
		chCount = rows[0].Cnt
	}

	diff := pgCount - chCount

	return &ReconcileResult{
		Day:             day.Format("2006-01-02"),
		PostgresCount:   pgCount,
		ClickHouseCount: chCount,
		Diff:            diff,
		InSync:          diff == 0,
	}, nil
}
