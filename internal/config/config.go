package config

import (
	"os"
	"strconv"
	"strings"
)

type Config struct {
	AveVisitorID                                                                                                       string
	Env, Port, DatabaseURL, RedisURL, RPCURL, RPCWSURL, ChainName, AveBaseURL, AveXAuth                                string
	ChainID                                                                                                            int64
	ExportPassword, EncryptionKey, StoragePath, StorageDriver, FixedGasPriceWei                                        string
	MaxPhotoSizeMB                                                                                                     int64
	AllowedPhotoTypes                                                                                                  string
	DeploymentListener, SwapListener                                                                                   bool
	PoolManager, UniversalRouter, Permit2, PonsFactory, PonsExternalHooks, NativeUSDPrice                              string
	MinEventUSD                                                                                                        float64
	WSSIngressBuffer, WSSStrategyBuffer, StrategyWorkers, StrategyQueueBuffer, StrategyShardBacklog, WSSReadLimitBytes int
}

func env(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
func boolEnv(k string, d bool) bool {
	v := strings.ToLower(os.Getenv(k))
	if v == "" {
		return d
	}
	return v == "1" || v == "true" || v == "yes"
}
func intEnv(k string, d int) int {
	return intEnvMax(k, d, 1_000_000)
}
func intEnvMax(k string, d, max int) int {
	v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(k)))
	if err != nil || v <= 0 || v > max {
		return d
	}
	return v
}
func floatEnv(k string, d float64) float64 {
	v, err := strconv.ParseFloat(strings.TrimSpace(os.Getenv(k)), 64)
	if err != nil || v <= 0 {
		return d
	}
	return v
}
func Load() Config {
	cid, _ := strconv.ParseInt(env("CHAIN_ID", "4663"), 10, 64)
	mb, _ := strconv.ParseInt(env("MAX_PHOTO_SIZE_MB", "8"), 10, 64)
	return Config{
		AveVisitorID: strings.TrimSpace(env("AVE_VISITOR_ID", "")),
		Env:          env("APP_ENV", "production"), Port: env("PORT", "3000"), DatabaseURL: env("DATABASE_URL", "postgres://robinhood:robinhood@localhost:5433/robinhood?sslmode=disable"), RedisURL: env("REDIS_URL", "redis://localhost:6380/0"), RPCURL: env("RPC_HTTP_URL", ""), RPCWSURL: env("RPC_WS_URL", ""), ChainID: cid, ChainName: env("CHAIN_NAME", "Robinhood"), AveBaseURL: strings.TrimRight(env("AVE_BASE_URL", "https://gj99z.com"), "/"), AveXAuth: env("AVE_X_AUTH", "9220492f383af7a1fbcfa74843ad121788573526984973724"), ExportPassword: env("ROBINHOOD_MONITOR_PRIVATE_KEY_EXPORT_PASSWORD", ""), EncryptionKey: env("PRIVATE_KEY_ENCRYPTION_KEY", "change-this-32-byte-key"), StoragePath: env("LOCAL_STORAGE_PATH", "./uploads"), StorageDriver: env("STORAGE_DRIVER", "local"), MaxPhotoSizeMB: mb, AllowedPhotoTypes: env("ALLOWED_PHOTO_TYPES", "image/jpeg,image/png,image/webp,image/heic"), DeploymentListener: boolEnv("PONS_V2_DEPLOYMENT_LISTENER_ENABLED", false), SwapListener: boolEnv("UNISWAP_V4_SWAP_LISTENER_ENABLED", true), PoolManager: strings.ToLower(env("UNISWAP_V4_POOL_MANAGER", "0x8366a39cc670b4001a1121b8f6a443a643e40951")), UniversalRouter: strings.ToLower(env("UNISWAP_V4_UNIVERSAL_ROUTER_ADDRESS", "0x8876789976decbfcbbbe364623c63652db8c0904")), Permit2: strings.ToLower(env("PERMIT2_ADDRESS", "0x000000000022d473030f116ddee9f6b43ac78ba3")), PonsFactory: strings.ToLower(env("PONS_V2_FACTORY_ADDRESS", "0x7ed598bcef8bd9edd8c97a195c6d13f40801ec7e")), PonsExternalHooks: strings.ToLower(env("PONS_V2_EXTERNAL_HOOKS_ADDRESS", "0xe5e702641ea86f4ae6cc3cdaed2b886f976be044")), NativeUSDPrice: env("ROBINHOOD_CHAIN_NATIVE_USD_PRICE", "0"), FixedGasPriceWei: strings.TrimSpace(env("TRADING_FIXED_GAS_PRICE_WEI", "60000000")),
		MinEventUSD: floatEnv("STRATEGY_MIN_EVENT_USD", 10), WSSIngressBuffer: intEnv("WSS_INGRESS_BUFFER", 32768), WSSStrategyBuffer: intEnv("WSS_STRATEGY_BUFFER", 8192), StrategyWorkers: intEnv("STRATEGY_WORKERS", 16), StrategyQueueBuffer: intEnv("STRATEGY_QUEUE_BUFFER", 512), StrategyShardBacklog: intEnv("STRATEGY_SHARD_BACKLOG", 2048), WSSReadLimitBytes: intEnvMax("WSS_READ_LIMIT_BYTES", 1<<20, 64<<20)}
}
