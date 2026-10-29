package service

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"tankard_workdir/internal/repository"
	redislib "tankard_workdir/lib/redis"
)

type RankedItem struct {
	ProductID int64   `json:"product_id"`
	Score     float64 `json:"score"`
}

type Ranker interface {
	Rank(ctx context.Context, userID int64, surface string, limit int) ([]RankedItem, error)
}

type MLRanker interface {
	Rank(ctx context.Context, userID int64, surface string, candidates []int64, mode string) ([]int64, error)
}

type Strategies struct {
	PersonalV1 Ranker
	PersonalV2 Ranker
	Popular    Ranker
}

type mlModeKey struct{}

func withMLMode(ctx context.Context, mode string) context.Context {
	if mode == "" {
		return ctx
	}
	return context.WithValue(ctx, mlModeKey{}, mode)
}

func mlModeFrom(ctx context.Context) string {
	mode, _ := ctx.Value(mlModeKey{}).(string)
	return mode
}

type PersonalRanker struct {
	log      *slog.Logger
	ml       MLRanker
	products *repository.ProductRepository
	variant  string
	reverse  bool
}

func NewPersonalRanker(
	log *slog.Logger,
	mlRanker MLRanker,
	products *repository.ProductRepository,
	variant string,
) *PersonalRanker {
	return &PersonalRanker{
		log:      log,
		ml:       mlRanker,
		products: products,
		variant:  variant,
		reverse:  variant == StrategyPersonalV2,
	}
}

// Rank передаёт все варианты ML-заглушке и возвращает порядок модели. Фильтрация наличия и бизнес-правил выполняется сервисом после ранжирования: модель не считается источником достоверных данных о доступности товара.
func (r *PersonalRanker) Rank(
	ctx context.Context,
	userID int64,
	surface string,
	limit int,
) ([]RankedItem, error) {
	candidates, err := r.products.AllIDs(ctx)
	if err != nil {
		return nil, fmt.Errorf("load candidates: %w", err)
	}

	ranked, err := r.ml.Rank(ctx, userID, surface, candidates, mlModeFrom(ctx))
	if err != nil {
		return nil, fmt.Errorf("%s rank: %w", r.variant, err)
	}

	if r.reverse {
		reverseInt64(ranked)
	}

	return toRanked(ranked), nil
}

type PopularRanker struct {
	log      *slog.Logger
	products *repository.ProductRepository
	profiles *repository.ProfileRepository
	cache    *redislib.Client
	cacheTTL time.Duration
}

func NewPopularRanker(
	log *slog.Logger,
	products *repository.ProductRepository,
	profiles *repository.ProfileRepository,
	cache *redislib.Client,
	cacheTTL time.Duration,
) *PopularRanker {
	return &PopularRanker{
		log:      log,
		products: products,
		profiles: profiles,
		cache:    cache,
		cacheTTL: cacheTTL,
	}
}

func (r *PopularRanker) Rank(
	ctx context.Context,
	userID int64,
	surface string,
	limit int,
) ([]RankedItem, error) {
	categories, err := r.profiles.GetTopCategories(ctx, userID)
	if err != nil {
		r.log.Warn("failed to load profile categories", "user_id", userID, "error", err)
	}

	key := popularCacheKey(surface, categories)
	if cached, err := r.load(ctx, key); err == nil && len(cached) > 0 {
		return cached, nil
	}

	products, err := r.products.PopularByCategories(ctx, categories, limit)
	if err != nil {
		return nil, fmt.Errorf("load popular products: %w", err)
	}

	items := make([]RankedItem, 0, len(products))
	for _, product := range products {
		items = append(items, RankedItem{ProductID: product.ID})
	}
	assignScores(items)

	r.store(ctx, key, items)

	return items, nil
}

func (r *PopularRanker) load(ctx context.Context, key string) ([]RankedItem, error) {
	raw, err := r.cache.Get(ctx, key)
	if err != nil {
		if errors.Is(err, redislib.NoMatches) {
			return nil, nil
		}
		return nil, err
	}

	var items []RankedItem
	if err := json.Unmarshal([]byte(raw), &items); err != nil {
		return nil, err
	}

	return items, nil
}

func (r *PopularRanker) store(ctx context.Context, key string, items []RankedItem) {
	data, err := json.Marshal(items)
	if err != nil {
		r.log.Warn("failed to marshal fallback cache", "error", err)
		return
	}

	if err := r.cache.SetWithExpiration(ctx, key, data, r.cacheTTL); err != nil {
		r.log.Warn("failed to store fallback cache", "error", err)
	}
}

// popularCacheKey сохраняет формат ключа резервного кэша: rec:fb:{surface}:{categories}.
func popularCacheKey(surface string, categories []string) string {
	label := "global"
	if len(categories) > 0 {
		label = strings.Join(categories, ",")
	}
	return fmt.Sprintf("rec:fb:%s:%s", surface, label)
}

// toRanked оборачивает идентификаторы товаров в элементы с убывающими позиционными оценками.
func toRanked(ids []int64) []RankedItem {
	items := make([]RankedItem, 0, len(ids))
	for i, id := range ids {
		items = append(items, RankedItem{ProductID: id, Score: rankScore(i)})
	}
	return items
}

// assignScores задаёт позиционные оценки элементам, для которых они не были рассчитаны при создании.
func assignScores(items []RankedItem) {
	for i := range items {
		if items[i].Score == 0 {
			items[i].Score = rankScore(i)
		}
	}
}

func reverseInt64(values []int64) {
	for i, j := 0, len(values)-1; i < j; i, j = i+1, j-1 {
		values[i], values[j] = values[j], values[i]
	}
}
