package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"score-updater-svc/internal/config"
	"score-updater-svc/internal/repository/kafka"
	"score-updater-svc/internal/repository/postgres"
	"score-updater-svc/internal/service"
	"score-updater-svc/internal/transport/router"
	"score-updater-svc/internal/transport/websockets"
	"score-updater-svc/pkg/logger"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
)

func main() {

	cfg, err := config.NewConfig()
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	SetupLogger(cfg)

	// Kafka configuration
	brokers := []string{cfg.Kafka.Broker}
	topic := cfg.Kafka.Topic
	groupId := cfg.Kafka.GroupId

	consumer, err := kafka.NewKafkaConsumer(brokers, topic, groupId)
	if err != nil {
		slog.Error("error: ", err.Error())
		//log.Fatalf("Failed to initialize Kafka consumer: %v", err)
	}

	rootCtx, rootCtxCancelFunc := context.WithCancel(context.Background())
	defer rootCtxCancelFunc()

	// initialize postgreSQL database connection
	dbclient, err := postgres.New(rootCtx, constructPostgresURL(cfg))
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}

	// initialize db abstraction layer to interact with database
	// dataRepo := postgres.NewDatabaseProvider(dbclient)
	// if err != nil {
	// 	log.Fatalf("Failed to initialize database: %v", err)
	// }

	// initialize repository layer that encapsulates db operations
	//postgresRepo := datastore.NewDataRepository(dataRepo)

	// initialize the message service
	messageService := service.NewMessageService(consumer)

	// initialize the score service
	//scoreService := service.NewScoreService(postgresRepo)

	// initialize the websocket handler
	wsHandler := websockets.NewWebSocketHandler(messageService)

	// Initialize HTTP server
	httpServer, err := NewHTTPServer(cfg, wsHandler)
	if err != nil {
		log.Fatalf("Failed to initialize HTTP server: %v", err)
	}

	// Graceful shutdown
	go initGracefulStop(rootCtxCancelFunc, httpServer, consumer, dbclient)
	<-rootCtx.Done()

}

func SetupLogger(cfg *config.ServiceConfig) {
	var level slog.Level
	if cfg.Logger.Level == "debug" {
		level = slog.LevelDebug
	} else {
		level = slog.LevelInfo
	}
	logCfg := logger.Config{
		Format: cfg.Logger.Format,
		Level:  level,
	}
	appLogger := logger.NewLogger(logCfg)
	slog.SetDefault(appLogger)
}

func NewHTTPServer(cfg *config.ServiceConfig, wsHandler *websockets.WebSocketHandler) (*http.Server, error) {

	// Create Gin router
	ginRouter := gin.New()

	// Register routes
	router.NewRouter(ginRouter, wsHandler)

	// Build HTTP server
	httpAddr := fmt.Sprintf(":%d", cfg.Port)
	server := &http.Server{
		Addr:    httpAddr,
		Handler: ginRouter,
	}

	// Run the server in a goroutine
	go func() {
		slog.Info("Starting HTTP server", slog.String("addr", httpAddr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("Failed to start HTTP server", slog.String("addr", httpAddr), slog.Any("error", err))
			os.Exit(1)
		}
	}()

	return server, nil
}

// initGracefulStop handles graceful shutdown
func initGracefulStop(rootCtxCancelFunc context.CancelFunc, httpServer *http.Server, producer *kafka.KafkaConsumer, pg *postgres.Client) {
	// Wait for stop signal
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	slog.Info("Service waiting for signal")
	sig := <-signals
	slog.Info("Service received signal", slog.Any("signal", sig))

	// Shutdown HTTP server
	slog.Info("Stopping HTTP server")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := httpServer.Shutdown(ctx); err != nil {
		slog.Error("Failed to shutdown HTTP server", slog.Any("error", err))
	}

	producer.CloseConnection()

	pg.Close()
	// Cancel root context
	rootCtxCancelFunc()
	slog.Info("Service stopped successfully")
}

func constructPostgresURL(dbConfig *config.ServiceConfig) string {
	return fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=%s",
		dbConfig.DbConfig.Username,
		dbConfig.DbConfig.Password,
		dbConfig.DbConfig.Host,
		dbConfig.DbConfig.Port,
		dbConfig.DbConfig.DbName,
		false,
	)
}
