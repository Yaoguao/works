package service

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"tankard_workdir/internal/models"
	"tankard_workdir/internal/repository"
	redislib "tankard_workdir/lib/redis"

	"golang.org/x/sync/singleflight"
)

const (
	SourceML       = "ml"
	SourceCache    = "cache"
	SourceFallback = "fallback"
	SourceStale    = "stale"
)

var errNoRecommendations = errors.New("no recommendations available")

type RecommendationConfig struct {
	Budget           time.Duration
	PersonalCacheTTL time.Duration
	StaleCacheTTL    time.Duration
	NegativeCacheTTL time.Duration
	DefaultLimit     int
	MaxLimit         int

	ExcludeLowMargin bool
}

type Item struct {
	ProductID int64   `json:"product_id"`
	Title     string  `json:"title"`
	Score     float64 `json:"score"`
}

type Result struct {
	RequestID   string
	UserID      int64
	Surface     string
	StrategyKey string
	Experiment  string
	Source      string
	Items       []Item
	LatencyMs   int
}

type RecommendParams struct {
	UserID  int64
	Surface string
	Limit   int
	MLMode  string
}

type cachedRecommendation struct {
	Items        []Item    `json:"items"`
	StrategyKey  string    `json:"strategy_key"`
	Experiment   string    `json:"experiment,omitempty"`
	ExperimentID int64     `json:"experiment_id,omitempty"`
	ExpiresAt    time.Time `json:"expires_at"`
}

func (c *cachedRecommendation) arm() ArmChoice {
	return ArmChoice{ExperimentID: c.ExperimentID, Experiment: c.Experiment, StrategyKey: c.StrategyKey}
}

type coldResult struct {
	items  []Item
	arm    ArmChoice
	source string
}

type strategyStep struct {
	key    string
	source string
	ranker Ranker
}

type RecommendationService struct {
	log        *slog.Logger
	cache      *redislib.Client
	router     *StrategyRouter
	strategies Strategies
	products   *repository.ProductRepository
	recLogs    *repository.RecommendationLogRepository
	recorder   ImpressionRecorder
	cfg        RecommendationConfig
	group      singleflight.Group
}

func NewRecommendationService(
	log *slog.Logger,
	cache *redislib.Client,
	router *StrategyRouter,
	strategies Strategies,
	products *repository.ProductRepository,
	recLogs *repository.RecommendationLogRepository,
	cfg RecommendationConfig,
	recorder ImpressionRecorder,
) *RecommendationService {
	return &RecommendationService{
		log:        log,
		cache:      cache,
		router:     router,
		strategies: strategies,
		products:   products,
		recLogs:    recLogs,
		cfg:        cfg,
		recorder:   recorder,
	}
}

// Recommend формирует рекомендации в порядке «кэш → стратегии → устаревшие данные» в рамках единого бюджета времени. Результат всегда проходит бизнес-фильтры: наличие товара и, при необходимости, маржинальность.
func (s *RecommendationService) Recommend(ctx context.Context, p RecommendParams) (Result, error) {
	start := time.Now()
	requestID := newRequestID()

	if p.Limit <= 0 {
		p.Limit = s.cfg.DefaultLimit
	}
	if p.Limit > s.cfg.MaxLimit {
		p.Limit = s.cfg.MaxLimit
	}

	ctx, cancel := context.WithTimeout(ctx, s.cfg.Budget)
	defer cancel()

	ctx = withMLMode(ctx, p.MLMode)

	choice := s.router.Pick(p.UserID)
	cacheKey := fmt.Sprintf("rec:%s:%d:%s", p.Surface, p.UserID, choice.StrategyKey)

	cached, err := s.loadCache(ctx, cacheKey)
	if err != nil {
		s.log.Warn("failed to read recommendation cache", "error", err)
	}

	if cached != nil && time.Now().Before(cached.ExpiresAt) {
		return s.finalize(ctx, start, requestID, p, cached.Items, cached.arm(), SourceCache), nil
	}

	v, err, _ := s.group.Do(cacheKey, func() (any, error) {
		return s.coldPath(ctx, p, cacheKey, choice, cached)
	})
	if err != nil {
		if cached != nil && len(cached.Items) > 0 {
			return s.finalize(ctx, start, requestID, p, cached.Items, cached.arm(), SourceStale), nil
		}
		return Result{}, fmt.Errorf("recommend: %w", err)
	}

	res := v.(coldResult)
	return s.finalize(ctx, start, requestID, p, res.items, res.arm, res.source), nil
}

// coldPath выполняется при промахе кэша; запросы для одной пары пользователь/поверхность объединяются через singleflight.
func (s *RecommendationService) coldPath(
	ctx context.Context,
	p RecommendParams,
	cacheKey string,
	choice ArmChoice,
	stale *cachedRecommendation,
) (coldResult, error) {
	skipML := s.negativeCached(ctx, p)
	if skipML {
		s.log.Debug("negative cache hit, skipping ML", "user_id", p.UserID, "surface", p.Surface)
	}

	res, ok := s.runChain(ctx, p, choice, skipML)
	if !ok {
		return s.staleOrFallback(p, stale)
	}

	s.storeCache(ctx, cacheKey, res.items, res.arm)

	return res, nil
}

// runChain проходит по цепочке резервных стратегий назначенной группы и возвращает первый непустой результат. Ошибка ML-стратегии включает отрицательное кэширование.
func (s *RecommendationService) runChain(
	ctx context.Context,
	p RecommendParams,
	choice ArmChoice,
	skipML bool,
) (coldResult, bool) {
	for _, step := range s.chainFor(choice.StrategyKey) {
		if skipML && step.source == SourceML {
			continue
		}

		ranked, err := step.ranker.Rank(ctx, p.UserID, p.Surface, s.cfg.MaxLimit)
		if err != nil {
			if step.source == SourceML {
				s.setNegative(ctx, p)
			}
			s.log.Warn("strategy step failed, degrading",
				"strategy_key", step.key,
				"user_id", p.UserID,
				"surface", p.Surface,
				"error", err,
			)
			continue
		}

		items, err := s.buildItems(ctx, ranked, s.cfg.MaxLimit)
		if err != nil {
			s.log.Warn("failed to build items", "strategy_key", step.key, "error", err)
			continue
		}
		if len(items) == 0 {
			continue
		}

		return coldResult{
			items:  items,
			arm:    s.armFor(choice, step.key),
			source: step.source,
		}, true
	}

	return coldResult{}, false
}

// chainFor задаёт цепочку резервирования: основная ML-стратегия, дополнительная стратегия, популярные товары из категорий и глобально популярные товары. Последние два шага выполняет PopularRanker.
func (s *RecommendationService) chainFor(strategyKey string) []strategyStep {
	switch strategyKey {
	case StrategyPersonalV2:
		return []strategyStep{
			{key: StrategyPersonalV2, source: SourceML, ranker: s.strategies.PersonalV2},
			{key: StrategyPersonalV1, source: SourceML, ranker: s.strategies.PersonalV1},
			{key: StrategyPopular, source: SourceFallback, ranker: s.strategies.Popular},
		}
	case StrategyPersonalV1:
		return []strategyStep{
			{key: StrategyPersonalV1, source: SourceML, ranker: s.strategies.PersonalV1},
			{key: StrategyPopular, source: SourceFallback, ranker: s.strategies.Popular},
		}
	default:
		return []strategyStep{
			{key: StrategyPopular, source: SourceFallback, ranker: s.strategies.Popular},
		}
	}
}

// armFor указывает экспериментальную группу стратегии, выдавшей результат. Для основной стратегии сохраняется назначенная пользователю группа; при деградации используется группа фактически сработавшей стратегии, чтобы популярные рекомендации учитывались как контроль.
func (s *RecommendationService) armFor(choice ArmChoice, strategyKey string) ArmChoice {
	if strategyKey == choice.StrategyKey {
		return choice
	}
	if arm, ok := s.router.ArmFor(strategyKey); ok {
		return ArmChoice{ExperimentID: arm.ID, Experiment: arm.Name, StrategyKey: arm.StrategyKey}
	}
	return ArmChoice{StrategyKey: strategyKey}
}

// staleOrFallback возвращает последние успешно полученные товары, если остальные варианты не сработали.
func (s *RecommendationService) staleOrFallback(p RecommendParams, stale *cachedRecommendation) (coldResult, error) {
	if stale != nil && len(stale.Items) > 0 {
		return coldResult{items: stale.Items, arm: stale.arm(), source: SourceStale}, nil
	}

	s.log.Warn("no recommendations resolved", "user_id", p.UserID, "surface", p.Surface)
	return coldResult{}, errNoRecommendations
}

// buildItems загружает сведения о товарах и применяет бизнес-фильтры после ранжирования. Товары не в наличии (103, 303) и, если включено соответствующее правило, товары с низкой маржинальностью не должны попасть в ответ.
func (s *RecommendationService) buildItems(ctx context.Context, ranked []RankedItem, limit int) ([]Item, error) {
	if len(ranked) == 0 {
		return nil, nil
	}

	ids := make([]int64, 0, len(ranked))
	for _, item := range ranked {
		ids = append(ids, item.ProductID)
	}

	products, err := s.products.ListByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}

	items := make([]Item, 0, limit)
	for _, rankedItem := range ranked {
		product, ok := products[rankedItem.ProductID]
		if !ok || !product.InStock || !s.marginAllowed(product.MarginBand) {
			continue
		}

		items = append(items, Item{
			ProductID: product.ID,
			Title:     product.Title,
			Score:     rankedItem.Score,
		})

		if len(items) >= limit {
			break
		}
	}

	return items, nil
}

// marginAllowed реализует необязательное правило маржинальности: приоритет бизнеса важнее предпочтений модели, поэтому при включённом флаге товары с низкой маржинальностью исключаются.
func (s *RecommendationService) marginAllowed(band string) bool {
	if !s.cfg.ExcludeLowMargin {
		return true
	}
	return band != "low"
}

// finalize ограничивает список заданным числом элементов, сохраняет журнал рекомендаций и регистрирует показы, после чего возвращает результат клиенту.
func (s *RecommendationService) finalize(
	ctx context.Context,
	start time.Time,
	requestID string,
	p RecommendParams,
	items []Item,
	arm ArmChoice,
	source string,
) Result {
	items = sliceItems(items, p.Limit)
	latencyMs := int(time.Since(start).Milliseconds())

	logCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), time.Second)
	defer cancel()

	recLog := &models.RecommendationLog{
		RequestID:   requestID,
		UserID:      p.UserID,
		Surface:     p.Surface,
		StrategyKey: arm.StrategyKey,
		ProductIDs:  productIDs(items),
		Source:      source,
		LatencyMs:   int32(latencyMs),
	}
	if arm.ExperimentID > 0 {
		recLog.ExperimentID = &arm.ExperimentID
	}

	if err := s.recLogs.Insert(logCtx, recLog); err != nil {
		s.log.Warn("failed to persist recommendation log", "error", err)
	}

	s.recordImpressions(requestID, p, items, arm.StrategyKey)

	s.log.Debug("recommendation served",
		"request_id", requestID,
		"user_id", p.UserID,
		"surface", p.Surface,
		"source", source,
		"strategy_key", arm.StrategyKey,
		"experiment", arm.Experiment,
		"latency_ms", latencyMs,
		"items", len(items),
	)

	return Result{
		RequestID:   requestID,
		UserID:      p.UserID,
		Surface:     p.Surface,
		StrategyKey: arm.StrategyKey,
		Experiment:  arm.Experiment,
		Source:      source,
		Items:       items,
		LatencyMs:   latencyMs,
	}
}

func (s *RecommendationService) loadCache(ctx context.Context, key string) (*cachedRecommendation, error) {
	raw, err := s.cache.Get(ctx, key)
	if err != nil {
		if errors.Is(err, redislib.NoMatches) {
			return nil, nil
		}
		return nil, err
	}

	var cached cachedRecommendation
	if err := json.Unmarshal([]byte(raw), &cached); err != nil {
		return nil, err
	}

	return &cached, nil
}

func (s *RecommendationService) storeCache(ctx context.Context, key string, items []Item, arm ArmChoice) {
	cached := cachedRecommendation{
		Items:        items,
		StrategyKey:  arm.StrategyKey,
		Experiment:   arm.Experiment,
		ExperimentID: arm.ExperimentID,
		ExpiresAt:    time.Now().Add(s.cfg.PersonalCacheTTL),
	}

	data, err := json.Marshal(cached)
	if err != nil {
		s.log.Warn("failed to marshal recommendation cache", "error", err)
		return
	}

	if err := s.cache.SetWithExpiration(ctx, key, data, s.cfg.StaleCacheTTL); err != nil {
		s.log.Warn("failed to store recommendation cache", "error", err)
	}
}

func (s *RecommendationService) negativeCached(ctx context.Context, p RecommendParams) bool {
	_, err := s.cache.Get(ctx, negativeKey(p))
	return err == nil
}

func (s *RecommendationService) setNegative(ctx context.Context, p RecommendParams) {
	if err := s.cache.SetWithExpiration(ctx, negativeKey(p), []byte("1"), s.cfg.NegativeCacheTTL); err != nil {
		s.log.Warn("failed to store negative cache", "error", err)
	}
}

func negativeKey(p RecommendParams) string {
	return fmt.Sprintf("rec:neg:%s:%d", p.Surface, p.UserID)
}

// rankScore назначает оценку, убывающую с позицией товара в списке.
func rankScore(position int) float64 {
	return 1.0 / float64(position+1)
}

func sliceItems(items []Item, limit int) []Item {
	if limit > 0 && len(items) > limit {
		return items[:limit]
	}
	return items
}

func productIDs(items []Item) []int64 {
	ids := make([]int64, len(items))
	for i, item := range items {
		ids[i] = item.ProductID
	}
	return ids
}

// recordImpressions передаёт регистратору каждый показанный товар. В отчёте контрольная группа (popular) и варианты воздействия (personal_v*) разделяются по стратегии, сформировавшей результат.
func (s *RecommendationService) recordImpressions(
	requestID string,
	p RecommendParams,
	items []Item,
	strategyKey string,
) {
	if s.recorder == nil || len(items) == 0 {
		return
	}

	now := time.Now()
	impressions := make([]Impression, 0, len(items))
	for _, item := range items {
		impressions = append(impressions, Impression{
			RequestID:   requestID,
			UserID:      p.UserID,
			Surface:     p.Surface,
			StrategyKey: strategyKey,
			ProductID:   item.ProductID,
			At:          now,
		})
	}

	s.recorder.RecordImpressions(impressions)
}

func newRequestID() string {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(buf)
}
