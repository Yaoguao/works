package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	httpapp "tankard_workdir/internal/app/http"
	"tankard_workdir/internal/config"
	"tankard_workdir/internal/http-server/handlers/admin"
	"tankard_workdir/internal/http-server/handlers/analytics"
	"tankard_workdir/internal/http-server/handlers/events"
	"tankard_workdir/internal/http-server/handlers/health"
	"tankard_workdir/internal/http-server/handlers/mlrank"
	"tankard_workdir/internal/http-server/handlers/recommendations"
	"tankard_workdir/internal/ml"
	"tankard_workdir/internal/repository"
	"tankard_workdir/internal/service"
	clickhouselib "tankard_workdir/lib/clickhouse"
	pkgkafka "tankard_workdir/lib/kafka"
	"tankard_workdir/lib/kafka/dlq"
	pgxdriver "tankard_workdir/lib/pgx-driver"
	"tankard_workdir/lib/pgx-driver/transaction"
	redislib "tankard_workdir/lib/redis"

	"github.com/gin-gonic/gin"
)

type App struct {
	log        *slog.Logger
	cfg        *config.Config
	httpServer *httpapp.App

	pg          *pgxdriver.Postgres
	redis       *redislib.Client
	clickhouse  *clickhouselib.Client
	producer    *pkgkafka.Producer
	dlqProducer *pkgkafka.Producer
	consumer    *pkgkafka.Consumer
	processor   *pkgkafka.Processor
	eventSvc    *service.EventService
	recSvc      *service.RecommendationService
	analytics   *service.AnalyticsService

	cancel context.CancelFunc
}

// New собирает граф зависимостей приложения: базу данных, репозитории, сервисы, производителя и потребителя Kafka, а также HTTP-сервер.
func New(log *slog.Logger, cfg *config.Config) (*App, error) {
	pg, err := pgxdriver.New(
		cfg.Postgres.DSN(),
		log,
		pgxdriver.MaxPoolSize(int32(cfg.Postgres.MaxOpenConns)),
		pgxdriver.MinConns(int32(cfg.Postgres.MaxIdleConns)),
	)
	if err != nil {
		return nil, fmt.Errorf("init postgres: %w", err)
	}

	tm, err := transaction.NewManager(pg, log)
	if err != nil {
		return nil, fmt.Errorf("init transaction manager: %w", err)
	}

	userRepo := repository.NewUserRepository(pg)
	productRepo := repository.NewProductRepository(pg)
	profileRepo := repository.NewProfileRepository(pg)
	recLogRepo := repository.NewRecommendationLogRepository(pg)
	experimentRepo := repository.NewExperimentRepository(pg)

	cache := redislib.New(cfg.Redis.Addr(), cfg.Redis.Password, cfg.Redis.DB)
	if err := cache.Ping(context.Background()); err != nil {
		log.Warn("redis is not reachable at startup", "addr", cfg.Redis.Addr(), "error", err)
	}

	ch := clickhouselib.New(
		cfg.ClickHouse.Host, cfg.ClickHouse.Port,
		cfg.ClickHouse.Database, cfg.ClickHouse.User, cfg.ClickHouse.Password,
	)
	if err := ch.Ping(context.Background()); err != nil {
		log.Warn("clickhouse is not reachable at startup",
			"addr", cfg.ClickHouse.Host, "database", cfg.ClickHouse.Database, "error", err)
	}

	analyticsSvc := service.NewAnalyticsService(
		log,
		repository.NewClickHouseAnalyticsRepository(ch),
		repository.NewAnalyticsRepository(pg),
	)
	if err := analyticsSvc.LoadExperiments(context.Background()); err != nil {
		log.Warn("failed to load experiments", "error", err)
	}

	ranker := ml.NewHTTPRanker(cfg.ML.URL, parseDuration(cfg.Recommendations.MLTimeout))

	strategyRouter := service.NewStrategyRouter(log, experimentRepo)
	if err := strategyRouter.Reload(context.Background()); err != nil {
		log.Warn("failed to load strategy config", "error", err)
	}

	strategies := service.Strategies{
		PersonalV1: service.NewPersonalRanker(log, ranker, productRepo, service.StrategyPersonalV1),
		PersonalV2: service.NewPersonalRanker(log, ranker, productRepo, service.StrategyPersonalV2),
		Popular: service.NewPopularRanker(
			log, productRepo, profileRepo, cache,
			parseDuration(cfg.Recommendations.FallbackCacheTTL),
		),
	}

	recSvc := service.NewRecommendationService(
		log, cache, strategyRouter, strategies, productRepo, recLogRepo,
		service.RecommendationConfig{
			Budget:           parseDuration(cfg.Recommendations.Budget),
			PersonalCacheTTL: parseDuration(cfg.Recommendations.PersonalCacheTTL),
			StaleCacheTTL:    parseDuration(cfg.Recommendations.StaleCacheTTL),
			NegativeCacheTTL: parseDuration(cfg.Recommendations.NegativeCacheTTL),
			DefaultLimit:     cfg.Recommendations.DefaultLimit,
			MaxLimit:         cfg.Recommendations.MaxLimit,
			ExcludeLowMargin: cfg.Recommendations.ExcludeLowMargin,
		},
		analyticsSvc,
	)

	producer := pkgkafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.Topic, *log)
	dlqProducer := pkgkafka.NewProducer(cfg.Kafka.Brokers, cfg.Kafka.DLQTopic, *log)

	eventSvc := service.NewEventService(
		log, producer, tm, userRepo, productRepo, profileRepo,
		analyticsSvc,
	)

	consumer := pkgkafka.NewConsumer(
		cfg.Kafka.Brokers, cfg.Kafka.Topic, cfg.Kafka.GroupID, *log,
	)

	processor, err := pkgkafka.NewProcessor(
		consumer,
		dlq.New(dlqProducer, *log),
		*log,
		pkgkafka.MaxAttempts(cfg.Kafka.MaxAttempts),
		pkgkafka.BaseRetryDelay(parseDuration(cfg.Kafka.BaseRetryDelay)),
		pkgkafka.MaxRetryDelay(parseDuration(cfg.Kafka.MaxRetryDelay)),
	)
	if err != nil {
		return nil, fmt.Errorf("init kafka processor: %w", err)
	}

	httpServer := httpapp.New(
		log,
		cfg.HTTP.Addr(),
		parseDuration(cfg.HTTP.ReadTimeout),
		parseDuration(cfg.HTTP.WriteTimeout),
		parseDuration(cfg.HTTP.IdleTimeout),
		buildRouter(log, eventSvc, recSvc, analyticsSvc, strategyRouter, parseDuration(cfg.ML.SlowDelay)),
	)

	return &App{
		log:         log,
		cfg:         cfg,
		httpServer:  httpServer,
		pg:          pg,
		redis:       cache,
		clickhouse:  ch,
		producer:    producer,
		dlqProducer: dlqProducer,
		consumer:    consumer,
		processor:   processor,
		eventSvc:    eventSvc,
		recSvc:      recSvc,
		analytics:   analyticsSvc,
	}, nil
}

// Run запускает потребителя Kafka и блокируется на HTTP-сервере.
func (a *App) Run() error {
	ctx, cancel := context.WithCancel(context.Background())
	a.cancel = cancel

	a.processor.Start(ctx, a.eventSvc.Handle)
	a.log.Info("kafka consumer started",
		"topic", a.cfg.Kafka.Topic,
		"group_id", a.cfg.Kafka.GroupID,
	)

	a.analytics.Start(ctx)

	return a.httpServer.Run()
}

// Stop корректно останавливает HTTP-сервер и все зависимые клиенты.
func (a *App) Stop(ctx context.Context) error {
	if a.cancel != nil {
		a.cancel()
	}

	var errs []error

	if err := a.httpServer.Stop(ctx); err != nil {
		errs = append(errs, err)
	}

	a.analytics.Stop(ctx)

	if err := a.consumer.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := a.producer.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := a.dlqProducer.Close(); err != nil {
		errs = append(errs, err)
	}
	if err := a.redis.Close(); err != nil {
		errs = append(errs, err)
	}

	a.pg.Close()

	return errors.Join(errs...)
}

func buildRouter(
	log *slog.Logger,
	eventSvc *service.EventService,
	recSvc *service.RecommendationService,
	analyticsSvc *service.AnalyticsService,
	strategyRouter *service.StrategyRouter,
	mlSlowDelay time.Duration,
) *gin.Engine {
	router := gin.New()
	router.Use(gin.Recovery())

	router.GET("/health", health.New())
	router.POST("/v1/events", events.New(log, eventSvc))
	router.GET("/v1/recommendations", recommendations.New(log, recSvc))
	router.POST("/ml/rank", mlrank.New(log, mlSlowDelay))

	router.GET("/v1/analytics/ab", analytics.NewAB(analyticsSvc))
	router.POST("/v1/analytics/ab/rebuild", analytics.NewRebuild(analyticsSvc))
	router.GET("/v1/analytics/reconcile", analytics.NewReconcile(analyticsSvc))

	router.PATCH("/v1/admin/experiments/:id", admin.NewPatchExperiment(strategyRouter))

	return router
}

func parseDuration(value string) time.Duration {
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return d
}
