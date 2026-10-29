package service

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"tankard_workdir/internal/repository"
)

const EventTypeImpression = "impression"

const (
	analyticsQueueSize = 4096
	analyticsBatchSize = 100
	analyticsFlushEach = time.Second

	attributionWindow = 30 * time.Minute

	eventTimeFormat = "2006-01-02 15:04:05.000"
)

var moscow = time.FixedZone("MSK", 3*60*60)

type Impression struct {
	RequestID   string
	UserID      int64
	Surface     string
	StrategyKey string
	ProductID   int64
	At          time.Time
}

type FunnelEvent struct {
	EventID      string
	UserID       int64
	EventType    string
	Surface      string
	ProductID    int64
	ExperimentID int64
	StrategyKey  string
	At           time.Time
}

type ImpressionRecorder interface {
	RecordImpressions(impressions []Impression)
}

type EventRecorder interface {
	RecordEvent(event FunnelEvent)
}

type ABRow struct {
	Day          string  `json:"day"`
	Surface      string  `json:"surface"`
	ExperimentID uint64  `json:"experiment_id"`
	StrategyKey  string  `json:"strategy_key"`
	Impressions  uint64  `json:"impressions"`
	Clicks       uint64  `json:"clicks"`
	Conversions  uint64  `json:"conversions"`
	CTR          float64 `json:"ctr"`
	CR           float64 `json:"cr"`
}

type ReconcileResult struct {
	Day              string `json:"day"`
	PGRequests       int64  `json:"pg_requests"`
	PGImpressions    int64  `json:"pg_impressions"`
	CHImpressions    uint64 `json:"ch_impressions"`
	ImpressionsDelta int64  `json:"impressions_delta"`
}

type AnalyticsService struct {
	log *slog.Logger
	ch  *repository.ClickHouseAnalyticsRepository
	pg  *repository.AnalyticsRepository

	queue   chan repository.RawEventRow
	dropped atomic.Int64

	experiments atomic.Value

	wg sync.WaitGroup
}

func NewAnalyticsService(
	log *slog.Logger,
	ch *repository.ClickHouseAnalyticsRepository,
	pg *repository.AnalyticsRepository,
) *AnalyticsService {
	return &AnalyticsService{
		log:   log,
		ch:    ch,
		pg:    pg,
		queue: make(chan repository.RawEventRow, analyticsQueueSize),
	}
}

// LoadExperiments кэширует идентификаторы экспериментов, чтобы помечать показы соответствующей экспериментальной группой.
func (s *AnalyticsService) LoadExperiments(ctx context.Context) error {
	experiments, err := s.pg.ExperimentByStrategy(ctx)
	if err != nil {
		return fmt.Errorf("load experiments: %w", err)
	}

	s.experiments.Store(experiments)
	s.log.Info("experiments loaded", "count", len(experiments))

	return nil
}

// RecordImpressions помещает в очередь по одной записи для каждого рекомендованного товара.
func (s *AnalyticsService) RecordImpressions(impressions []Impression) {
	for _, impression := range impressions {
		at := impression.At
		if at.IsZero() {
			at = time.Now()
		}

		s.enqueue(repository.RawEventRow{
			EventID:      fmt.Sprintf("imp:%s:%d", impression.RequestID, impression.ProductID),
			EventTime:    at.In(moscow).Format(eventTimeFormat),
			UserID:       uint64(impression.UserID),
			EventType:    EventTypeImpression,
			Surface:      impression.Surface,
			ProductID:    uint64(impression.ProductID),
			ExperimentID: s.experimentFor(impression.StrategyKey),
			StrategyKey:  impression.StrategyKey,
		})
	}
}

// RecordEvent помещает в очередь клик или покупку из конвейера обработки событий.
func (s *AnalyticsService) RecordEvent(event FunnelEvent) {
	at := event.At
	if at.IsZero() {
		at = time.Now()
	}

	experimentID := uint64(0)
	if event.ExperimentID > 0 {
		experimentID = uint64(event.ExperimentID)
	}

	s.enqueue(repository.RawEventRow{
		EventID:      "evt:" + event.EventID,
		EventTime:    at.In(moscow).Format(eventTimeFormat),
		UserID:       uint64(event.UserID),
		EventType:    event.EventType,
		Surface:      event.Surface,
		ProductID:    uint64(event.ProductID),
		ExperimentID: experimentID,
		StrategyKey:  event.StrategyKey,
	})
}

// Start запускает фоновую отправку накопленных событий.
func (s *AnalyticsService) Start(ctx context.Context) {
	s.wg.Add(1)
	go s.run(ctx)
}

// Stop ожидает отправки накопленных данных и завершения фонового процесса.
func (s *AnalyticsService) Stop(ctx context.Context) {
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
	case <-ctx.Done():
	}
}

// BuildABDaily перестраивает tankard_analytics.ab_daily по исходным событиям.
func (s *AnalyticsService) BuildABDaily(ctx context.Context) error {
	if err := s.ch.RebuildABDaily(ctx); err != nil {
		return fmt.Errorf("rebuild ab_daily: %w", err)
	}

	s.log.Info("ab_daily rebuilt")

	return nil
}

// Report читает дневной агрегат и вычисляет CTR и CR для каждой экспериментальной группы.
func (s *AnalyticsService) Report(
	ctx context.Context,
	from, to time.Time,
	surface string,
) ([]ABRow, error) {
	rows, err := s.ch.SelectABDaily(ctx, from, to, surface)
	if err != nil {
		return nil, err
	}

	report := make([]ABRow, 0, len(rows))
	for _, row := range rows {
		report = append(report, ABRow{
			Day:          row.Day,
			Surface:      row.Surface,
			ExperimentID: row.ExperimentID,
			StrategyKey:  row.StrategyKey,
			Impressions:  row.Impressions,
			Clicks:       row.Clicks,
			Conversions:  row.Conversions,
			CTR:          ratio(row.Clicks, row.Impressions),
			CR:           ratio(row.Conversions, row.Impressions),
		})
	}

	return report, nil
}

// Reconcile сравнивает recommendation_logs в OLTP с показами в OLAP за указанный день.
func (s *AnalyticsService) Reconcile(ctx context.Context, day time.Time) (ReconcileResult, error) {
	requests, impressions, err := s.pg.RecommendationLogsByDay(ctx, day)
	if err != nil {
		return ReconcileResult{}, err
	}

	chImpressions, err := s.ch.CountImpressionsByDay(ctx, day)
	if err != nil {
		return ReconcileResult{}, err
	}

	return ReconcileResult{
		Day:              day.Format("2006-01-02"),
		PGRequests:       requests,
		PGImpressions:    impressions,
		CHImpressions:    chImpressions,
		ImpressionsDelta: int64(chImpressions) - impressions,
	}, nil
}

// run обрабатывает очередь: отправляет пакет при достижении его размера, по таймеру или при завершении работы.
func (s *AnalyticsService) run(ctx context.Context) {
	defer s.wg.Done()

	ticker := time.NewTicker(analyticsFlushEach)
	defer ticker.Stop()

	batch := make([]repository.RawEventRow, 0, analyticsBatchSize)

	flush := func() {
		if len(batch) == 0 {
			return
		}
		s.flush(batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			for {
				select {
				case row := <-s.queue:
					batch = append(batch, row)
					if len(batch) >= analyticsBatchSize {
						flush()
					}
				default:
					flush()
					s.log.Info("analytics flusher stopped",
						"dropped_total", s.dropped.Load(),
					)
					return
				}
			}
		case row := <-s.queue:
			batch = append(batch, row)
			if len(batch) >= analyticsBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// flush связывает ожидающие клики с показами и записывает пакет.
func (s *AnalyticsService) flush(rows []repository.RawEventRow) {
	insertCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	s.attribute(insertCtx, rows)

	if err := s.ch.InsertEvents(insertCtx, rows); err != nil {
		s.log.Warn("failed to flush analytics events", "rows", len(rows), "error", err)
	}
}

// attribute находит показ для событий воронки, пришедших без данных об эксперименте, по поверхности, пользователю, товару и временному интервалу.
func (s *AnalyticsService) attribute(ctx context.Context, rows []repository.RawEventRow) {
	for i := range rows {
		row := &rows[i]
		if row.ExperimentID != 0 || row.StrategyKey != "" {
			continue
		}
		if row.EventType != EventTypeClick && row.EventType != EventTypePurchase {
			continue
		}

		experimentID, strategyKey, ok, err := s.ch.LatestImpressionExperiment(
			ctx, int64(row.UserID), int64(row.ProductID), row.Surface, attributionWindow,
		)
		if err != nil {
			s.log.Warn("failed to attribute event to impression", "event_id", row.EventID, "error", err)
			continue
		}
		if ok {
			row.ExperimentID = experimentID
			row.StrategyKey = strategyKey
		}
	}
}

// enqueue помещает запись в очередь, не блокируя вызывающую сторону; при переполнении запись отбрасывается.
func (s *AnalyticsService) enqueue(row repository.RawEventRow) {
	select {
	case s.queue <- row:
	default:
		if dropped := s.dropped.Add(1); dropped%100 == 1 {
			s.log.Warn("analytics queue is full, dropping event", "dropped_total", dropped)
		}
	}
}

func (s *AnalyticsService) experimentFor(strategyKey string) uint64 {
	experiments, ok := s.experiments.Load().(map[string]int64)
	if !ok || experiments == nil {
		return 0
	}

	id, ok := experiments[strategyKey]
	if !ok {
		return 0
	}

	return uint64(id)
}

// ratio предотвращает деление на ноль: без показов метрика не определяется.
func ratio(part, total uint64) float64 {
	if total == 0 {
		return 0
	}
	return float64(part) / float64(total)
}
