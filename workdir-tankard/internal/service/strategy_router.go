package service

import (
	"context"
	"fmt"
	"hash/fnv"
	"log/slog"
	"strconv"
	"sync/atomic"

	"tankard_workdir/internal/models"
	"tankard_workdir/internal/repository"
)

const (
	StrategyPopular    = "popular"
	StrategyPersonalV1 = "personal_v1"
	StrategyPersonalV2 = "personal_v2"
)

const bucketCount = 100

type ExperimentArm struct {
	ID          int64  `json:"id"`
	Name        string `json:"name"`
	StrategyKey string `json:"strategy_key"`
	Weight      int    `json:"weight"`
}

type ArmChoice struct {
	ExperimentID int64
	Experiment   string
	StrategyKey  string
}

type strategyConfig struct {
	arms       []ExperimentArm
	cumulative []int
	control    ExperimentArm
	hasControl bool
}

type StrategyRouter struct {
	log  *slog.Logger
	repo *repository.ExperimentRepository
	cfg  atomic.Value
}

func NewStrategyRouter(log *slog.Logger, repo *repository.ExperimentRepository) *StrategyRouter {
	return &StrategyRouter{log: log, repo: repo}
}

// Reload заново загружает из Postgres снимок маршрутизации в памяти. Вызывается при запуске и после каждого изменения через админ API, поэтому новые веса применяются к следующему запросу.
func (r *StrategyRouter) Reload(ctx context.Context) error {
	experiments, err := r.repo.List(ctx)
	if err != nil {
		return fmt.Errorf("list experiments: %w", err)
	}

	cfg := buildStrategyConfig(experiments)
	r.cfg.Store(cfg)
	r.log.Info("strategy config reloaded", "arms", len(cfg.arms))

	return nil
}

// Update сохраняет частичное изменение экспериментальной группы и обновляет снимок маршрутизации.
func (r *StrategyRouter) Update(ctx context.Context, id int64, weight *int32, enabled *bool) error {
	if err := r.repo.Update(ctx, id, weight, enabled); err != nil {
		return err
	}
	if err := r.Reload(ctx); err != nil {
		return fmt.Errorf("reload after update: %w", err)
	}

	r.log.Info("experiment updated", "experiment_id", id, "enabled", enabled, "weight", weight)
	return nil
}

// Arms возвращает все включённые экспериментальные группы, включая контрольную.
func (r *StrategyRouter) Arms() []ExperimentArm {
	return append([]ExperimentArm(nil), r.config().arms...)
}

// ArmFor возвращает включённую группу с заданным ключом стратегии. Это позволяет связать деградировавший ответ с группой, которая фактически его сформировала.
func (r *StrategyRouter) ArmFor(strategyKey string) (ExperimentArm, bool) {
	for _, arm := range r.config().arms {
		if arm.StrategyKey == strategyKey {
			return arm, true
		}
	}
	return ExperimentArm{}, false
}

// Pick назначает пользователя экспериментальной группе по хешу user_id и накопленным весам. Отключённый вариант или вариант с нулевым весом удаляется из снимка, поэтому его трафик сразу перенаправляется в контрольную группу.
func (r *StrategyRouter) Pick(userID int64) ArmChoice {
	cfg := r.config()

	if len(cfg.arms) == 0 {
		return ArmChoice{StrategyKey: StrategyPopular}
	}

	bucket := int(bucketHash(userID) % bucketCount)
	for i, edge := range cfg.cumulative {
		if bucket < edge {
			arm := cfg.arms[i]
			return ArmChoice{ExperimentID: arm.ID, Experiment: arm.Name, StrategyKey: arm.StrategyKey}
		}
	}

	if cfg.hasControl {
		return ArmChoice{
			ExperimentID: cfg.control.ID,
			Experiment:   cfg.control.Name,
			StrategyKey:  cfg.control.StrategyKey,
		}
	}

	return ArmChoice{StrategyKey: StrategyPopular}
}

func (r *StrategyRouter) config() strategyConfig {
	cfg, ok := r.cfg.Load().(strategyConfig)
	if !ok {
		return strategyConfig{}
	}
	return cfg
}

// buildStrategyConfig оставляет включённые группы с положительным весом, помещает контрольную группу первой и вычисляет границы накопленных весов для распределения трафика.
func buildStrategyConfig(experiments []models.RecommendationExperiment) strategyConfig {
	var (
		control    ExperimentArm
		others     []ExperimentArm
		hasControl bool
	)

	for _, e := range experiments {
		if !e.Enabled || e.Weight <= 0 {
			continue
		}

		arm := ExperimentArm{
			ID:          e.ID,
			Name:        e.Name,
			StrategyKey: e.StrategyKey,
			Weight:      int(e.Weight),
		}

		if !hasControl && e.StrategyKey == StrategyPopular {
			control, hasControl = arm, true
			continue
		}
		others = append(others, arm)
	}

	arms := make([]ExperimentArm, 0, len(others)+1)
	if hasControl {
		arms = append(arms, control)
	}
	arms = append(arms, others...)

	cumulative := make([]int, len(arms))
	total := 0
	for i, arm := range arms {
		total += arm.Weight
		cumulative[i] = total
	}

	return strategyConfig{arms: arms, cumulative: cumulative, control: control, hasControl: hasControl}
}

// bucketHash вычисляет стабильную корзину по идентификатору пользователя. FNV-1a от его десятичного представления даёт одинаковый результат между процессами и перезапусками, что необходимо для закрепления пользователя за группой.
func bucketHash(userID int64) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(strconv.FormatInt(userID, 10)))
	return h.Sum64()
}
