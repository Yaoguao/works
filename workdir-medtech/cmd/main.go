package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"medtech-practice/internal/config"
	http_server "medtech-practice/internal/http-server"
	"medtech-practice/internal/notifier"
	"medtech-practice/internal/services"
	"medtech-practice/internal/storage"
	"medtech-practice/internal/worker"
	"medtech-practice/lib/clickhouse"
	"medtech-practice/lib/logger/sl"
	"medtech-practice/lib/mlclient"
	pgx_driver "medtech-practice/lib/pgx-driver"
	"medtech-practice/lib/pgx-driver/transaction"
	"medtech-practice/lib/rabbitmq"
	redislib "medtech-practice/lib/redis"
)

const shutdownTimeout = 10 * time.Second

func main() {
	cfg := config.MustLoad()

	log := sl.InitLogger(cfg.Env, os.Stdout)

	pg, err := pgx_driver.New(postgresDSN(cfg.Postgres), log,
		pgx_driver.MaxPoolSize(int32(cfg.Postgres.MaxOpenConns)),
		pgx_driver.MinConns(int32(cfg.Postgres.MaxIdleConns)),
	)
	if err != nil {
		log.Error("failed to connect to postgres", sl.Err(err))
		os.Exit(1)
	}
	defer pg.Close()

	rdb := redislib.New(
		fmt.Sprintf("%s:%d", cfg.Redis.Host, cfg.Redis.Port),
		cfg.Redis.Password,
		cfg.Redis.DB,
	)
	if err := rdb.Ping(context.Background()); err != nil {
		log.Error("failed to connect to redis", sl.Err(err))
		os.Exit(1)
	}
	defer rdb.Close()

	txManager, err := transaction.NewManager(pg, log)
	if err != nil {
		log.Error("failed to create transaction manager", sl.Err(err))
		os.Exit(1)
	}

	apptStorage := storage.New(pg)
	bookingService := services.NewBookingService(pg, txManager, apptStorage, rdb, log)

	minioClient, err := minio.New(cfg.MinIO.Endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.MinIO.AccessKey, cfg.MinIO.SecretKey, ""),
		Secure: cfg.MinIO.UseSSL,
	})
	if err != nil {
		log.Error("failed to create minio client", sl.Err(err))
		os.Exit(1)
	}
	cancelHealthCheck, err := minioClient.HealthCheck(5 * time.Second)
	if err != nil {
		log.Error("failed to start minio health check", sl.Err(err))
		os.Exit(1)
	}
	defer cancelHealthCheck()

	if !minioClient.IsOnline() {
		log.Error("minio is not available")
		os.Exit(1)
	}

	presignTTL := time.Duration(cfg.MinIO.PresignTTLSeconds) * time.Second
	if presignTTL <= 0 {
		presignTTL = 120 * time.Second
	}
	documentsService := services.NewDocumentsService(
		pg,
		storage.NewDocumentStorage(pg),
		minioClient,
		services.DocumentsConfig{
			BucketConsents:    cfg.MinIO.BucketConsents,
			BucketAttachments: cfg.MinIO.BucketAttachments,
			PresignTTL:        presignTTL,
		},
		log,
	)

	chClient := clickhouse.New(cfg.ClickHouse.Host, cfg.ClickHouse.Port, cfg.ClickHouse.User, cfg.ClickHouse.Password)
	analyticsService := services.NewAnalyticsService(pg, storage.NewAnalyticsStorage(pg), chClient, log)

	mlEndpoint := cfg.ML.Endpoint
	if mlEndpoint == "" {
		mlEndpoint = fmt.Sprintf("http://127.0.0.1:%d", cfg.HTTP.Port)
	}
	mlClient := mlclient.New(mlEndpoint, mustDuration(cfg.ML.Timeout, 3*time.Second))

	scoringService := services.NewScoringService(pg, apptStorage, mlClient, log)
	proposalService := services.NewProposalService(pg, txManager, storage.NewProposalStorage(pg), apptStorage, log)

	rabbitClient, err := rabbitmq.NewClient(rabbitmq.ClientConfig{
		URL:            cfg.RabbitMQ.URL,
		ConnectionName: "medtech-api",
		ConnectTimeout: 5 * time.Second,
		Heartbeat:      10 * time.Second,
		ReconnectStrat: rabbitmq.Strategy{Attempts: 3, Delay: time.Second, Backoff: 2},
		ProducingStrat: rabbitmq.Strategy{Attempts: 3, Delay: 200 * time.Millisecond, Backoff: 2},
	})
	if err != nil {
		log.Error("failed to connect to rabbitmq", sl.Err(err))
		os.Exit(1)
	}
	defer func() {
		if err := rabbitClient.Close(); err != nil {
			log.Error("failed to close rabbitmq client", sl.Err(err))
		}
	}()

	notificationsQueue := cfg.RabbitMQ.Queue
	if notificationsQueue == "" {
		notificationsQueue = worker.NotificationQueue
	}
	publisher, err := notifier.New(rabbitClient, notificationsQueue, log)
	if err != nil {
		log.Error("failed to declare rabbitmq topology", sl.Err(err))
		os.Exit(1)
	}

	outboxService := services.NewOutboxService(pg, apptStorage)

	workerCtx, stopWorkers := context.WithCancel(context.Background())
	defer stopWorkers()
	go worker.NewScoringWorker(scoringService, 5*time.Second, 50, log).Run(workerCtx)
	go worker.NewExpireWorker(proposalService, 30*time.Second, log).Run(workerCtx)
	go worker.NewOutboxWorker(outboxService, publisher, notificationsQueue, 2*time.Second, 100, log).Run(workerCtx)

	router := http_server.New(cfg.Env, log, bookingService, documentsService, analyticsService, proposalService)

	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", cfg.HTTP.Host, cfg.HTTP.Port),
		Handler:      router,
		ReadTimeout:  mustDuration(cfg.HTTP.ReadTimeout, 5*time.Second),
		WriteTimeout: mustDuration(cfg.HTTP.WriteTimeout, 10*time.Second),
		IdleTimeout:  mustDuration(cfg.HTTP.IdleTimeout, 60*time.Second),
	}

	stopped := make(chan struct{})
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
		<-sig

		ctx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()

		if err := srv.Shutdown(ctx); err != nil {
			log.Error("graceful shutdown failed", sl.Err(err))
		}
		close(stopped)
	}()

	log.Info("server started", slog.String("addr", srv.Addr))

	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server failed", sl.Err(err))
		os.Exit(1)
	}

	<-stopped
	log.Info("server stopped")
}

func postgresDSN(cfg config.Postgres) string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Database, cfg.SSLMode,
	)
}

func mustDuration(s string, fallback time.Duration) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil || d <= 0 {
		return fallback
	}
	return d
}
