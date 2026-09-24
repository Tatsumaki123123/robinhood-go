package main

import (
	"context"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/compress"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/gofiber/fiber/v2/middleware/recover"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"os"
	"os/signal"
	"robinhood-go/internal/api"
	"robinhood-go/internal/chain"
	"robinhood-go/internal/config"
	"robinhood-go/internal/store"
	"robinhood-go/internal/strategy"
	"syscall"
	"time"
)

func main() {
	cfg := config.Load()
	log, _ := zap.NewProduction()
	defer log.Sync()
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	st, err := store.New(ctx, cfg.DatabaseURL)
	if err != nil {
		log.Fatal("database", zap.Error(err))
	}
	defer st.Close()
	if err = st.Migrate(ctx); err != nil {
		log.Fatal("migration", zap.Error(err))
	}
	rpc := chain.NewWithEndpoints(cfg.RPCURL, cfg.RPCSendURL, cfg.RPCWSURL, chain.Options{IngressBuffer: cfg.WSSIngressBuffer, StrategyBuffer: cfg.WSSStrategyBuffer, ReadLimit: int64(cfg.WSSReadLimitBytes)})
	rpc.SetStrategyFilter(chain.StrategyEventFilter)
	rd := redis.NewClient(&redis.Options{Addr: redisAddr(cfg.RedisURL)})
	fixedGasPrice := chain.ToBig(cfg.FixedGasPriceWei)
	if fixedGasPrice.Sign() <= 0 {
		fixedGasPrice = nil
	}
	tr := &chain.Trading{RPC: rpc, ChainID: cfg.ChainID, Router: cfg.UniversalRouter, Permit2: cfg.Permit2, PoolManager: cfg.PoolManager, FixedGasPrice: fixedGasPrice}
	app := fiber.New(fiber.Config{AppName: "Robinhood Go", BodyLimit: 32 * 1024 * 1024, ErrorHandler: func(c *fiber.Ctx, e error) error {
		return c.Status(500).JSON(map[string]any{"statusCode": 500, "message": e.Error()})
	}})
	app.Use(recover.New(), compress.New(), cors.New(cors.Config{AllowOrigins: "*", AllowHeaders: "Origin, Content-Type, Accept, Authorization"}))
	app.Static("/uploads", cfg.StoragePath)
	app.Static("/docs", "docs")
	service := api.New(cfg, st, rpc, tr, rd, log)
	service.Register(app)
	service.StartAvePriceRefresh(ctx)
	service.StartAveDataRefresh(ctx)
	service.StartEventBridge()
	if cfg.SwapListener {
		rpc.Subscribe(ctx, map[string]any{"address": []string{cfg.PoolManager}, "topics": []string{"0x40e9cecb9f5f1f1c5b9c97dec2917b7ee92e57ba5563708daca94dd84ad7112f"}})
	}
	if cfg.DeploymentListener {
		rpc.Subscribe(ctx, map[string]any{"address": []string{"0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e"}})
	}
	engine := strategy.New(st, rpc)
	engine.ConfigureTrading(tr, cfg.EncryptionKey, cfg.NativeUSDPrice)
	engine.ConfigurePerformance(cfg.StrategyWorkers, cfg.StrategyQueueBuffer, cfg.StrategyShardBacklog)
	engine.ConfigureEventFilter(cfg.MinEventUSD)
	engine.ConfigureLogging(log, cfg.Env)
	service.SetMonitorCacheInvalidator(engine.InvalidateMonitorCache)
	service.SetStrategyMetrics(engine.Metrics)
	engine.CleanupExpiredPendingSells(ctx)
	go engine.Run(ctx)
	go engine.Tick(ctx)
	go func() { <-ctx.Done(); _ = app.Shutdown() }()
	log.Info("server started", zap.String("port", cfg.Port))
	if err := app.Listen(":" + cfg.Port); err != nil {
		log.Fatal("server", zap.Error(err))
	}
	time.Sleep(50 * time.Millisecond)
}
func redisAddr(url string) string {
	if len(url) > 8 && url[:8] == "redis://" {
		url = url[8:]
	}
	for i, c := range url {
		if c == '/' || c == '?' {
			return url[:i]
		}
	}
	return url
}
