package strategy

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
	"robinhood-go/internal/chain"
	secret "robinhood-go/internal/crypto"
	"robinhood-go/internal/store"
)

// Event is the normalized event consumed by the strategy engine.  The parser
// accepts both the legacy short names and the names emitted by the Node
// listener (quoteAmountRaw, priceImpactRatio, blockNumber).
type Event struct {
	Key              string    `json:"sourceEventKey"`
	UserID           int64     `json:"userId"`
	TokenAddress     string    `json:"tokenAddress"`
	CurveAddress     string    `json:"curveAddress"`
	PoolID           string    `json:"poolId"`
	Amount0Raw       string    `json:"amount0Raw"`
	Amount1Raw       string    `json:"amount1Raw"`
	Side             string    `json:"side"`
	QuoteUSD         float64   `json:"quoteUSD"`
	QuoteAmountRaw   float64   `json:"quoteAmountRaw"`
	TokenAmountRaw   float64   `json:"tokenAmountRaw"`
	QuoteAmountText  string    `json:"-"`
	TokenAmountText  string    `json:"-"`
	Impact           float64   `json:"impact"`
	PriceImpactRatio float64   `json:"priceImpactRatio"`
	PriceChangeRatio float64   `json:"priceChangeRatio"`
	SqrtPriceX96     string    `json:"sqrtPriceX96"`
	Price            float64   `json:"price"`
	PriceRaw         float64   `json:"priceRaw"`
	TokenDecimals    int       `json:"tokenDecimals"`
	QuoteDecimals    int       `json:"quoteDecimals"`
	MarketCap        float64   `json:"marketCap"`
	Block            string    `json:"block"`
	Own              bool      `json:"own"`
	Sender           string    `json:"sender"`
	ReceivedAt       time.Time `json:"receivedAt"`
	// ExecutionAmountRaw is an internal exact quantity hint for sell actions;
	// it is never serialized as part of the public event contract.
	ExecutionAmountRaw string `json:"-"`
}

type position struct {
	// AmountRaw is kept as an integer string because ERC-20 quantities commonly
	// exceed float64's 53-bit exact range. Amount remains a derived value used
	// by the existing strategy arithmetic and API-compatible callbacks.
	AmountRaw                                          string
	Amount, AverageCost, FirstPrice, LastPrice         float64
	BuyCount, SellCount, ProfitLevel                   int
	PendingSell                                        bool
	FirstBought, NextScheduled, CooldownUntil, LastBuy *time.Time
}

type Engine struct {
	Store *store.Store
	RPC   *chain.RPC
	log   *zap.Logger
	dev   bool
	locks sync.Map
	// ExecuteAction is injected by the server when live strategy trading is
	// enabled. It must return only after broadcast and must not
	// mutate strategy state; own-chain fill events call applyOwnFill after a
	// successful receipt. Keeping this hook optional preserves deterministic
	// replay/testing of the strategy engine.
	ExecuteAction  func(context.Context, Event, string, float64) (map[string]any, error)
	Trading        *chain.Trading
	EncryptionKey  string
	NativeUSDPrice string
	supplies       sync.Map // token address -> cached total supply raw
	curveBlocks    sync.Map // curve address -> last polled block
	pollMu         sync.Mutex
	monitorMu      sync.RWMutex
	monitorUsers   map[int64]store.User
	monitorTokens  map[int64][]store.Token
	monitorAt      time.Time
	monitorLoadMu  sync.Mutex
}

func New(s *store.Store, r *chain.RPC) *Engine {
	return &Engine{Store: s, RPC: r, monitorUsers: make(map[int64]store.User), monitorTokens: make(map[int64][]store.Token)}
}

// ConfigureLogging enables the strategy diagnostics only for development
// environments. Keeping the check in the engine prevents strategy event logs
// from being emitted by production deployments even when a logger is present.
func (e *Engine) ConfigureLogging(log *zap.Logger, env string) {
	e.log = log
	env = strings.ToLower(strings.TrimSpace(env))
	e.dev = env == "dev" || env == "development"
}

func (e *Engine) devInfo(message string, fields ...zap.Field) {
	if e.dev && e.log != nil {
		e.log.Info(message, fields...)
	}
}

func (e *Engine) ConfigureTrading(t *chain.Trading, encryptionKey string, nativeUSDPrice ...string) {
	e.Trading, e.EncryptionKey = t, encryptionKey
	if len(nativeUSDPrice) > 0 {
		e.NativeUSDPrice = strings.TrimSpace(nativeUSDPrice[0])
	}
}

// InvalidateMonitorCache is called by the HTTP mutation handlers after a
// user/token change so the next WSS event observes the new switch immediately
// instead of waiting for the normal two-second refresh interval.
func (e *Engine) InvalidateMonitorCache() {
	e.monitorMu.Lock()
	e.monitorAt = time.Time{}
	e.monitorMu.Unlock()
}

func (e *Engine) Run(ctx context.Context) {
	// Decode and evaluate independent pools in parallel. A stable shard key
	// keeps events for one user/token/pool in chain order, while unrelated
	// positions no longer wait behind a slow database or RPC call.
	const workerCount = 16
	queues := make([]chan map[string]any, workerCount)
	var wg sync.WaitGroup
	for i := range queues {
		queues[i] = make(chan map[string]any, 512)
		wg.Add(1)
		go func(queue <-chan map[string]any) {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case raw, ok := <-queue:
					if !ok {
						return
					}
					e.processDecoded(ctx, raw)
				}
			}
		}(queues[i])
	}
	for {
		select {
		case <-ctx.Done():
			for _, queue := range queues {
				close(queue)
			}
			wg.Wait()
			return
		case raw, ok := <-e.RPC.Events:
			if !ok {
				for _, queue := range queues {
					close(queue)
				}
				wg.Wait()
				return
			}
			idx := eventShard(raw, workerCount)
			select {
			case queues[idx] <- raw:
			case <-ctx.Done():
			}
		}
	}
}

func (e *Engine) processDecoded(ctx context.Context, raw map[string]any) {
	if ev, ok := decodeEvent(raw); ok {
		if ev.UserID > 0 {
			if err := e.Process(ctx, ev); err != nil {
				e.devInfo("策略处理失败",
					zap.Error(err),
					zap.Int64("userID", ev.UserID),
					zap.String("side", strings.ToLower(ev.Side)),
					zap.String("token", ev.TokenAddress),
					zap.String("curve", ev.CurveAddress),
				)
			}
		} else {
			if err := e.processForMonitors(ctx, ev); err != nil {
				e.devInfo("策略监听处理失败",
					zap.Error(err),
					zap.String("side", strings.ToLower(ev.Side)),
					zap.String("token", ev.TokenAddress),
					zap.String("curve", ev.CurveAddress),
				)
			}
		}
	}
}

func eventShard(raw map[string]any, workers int) int {
	if workers <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(strings.ToLower(firstString(raw, "userId", "userID"))))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(strings.ToLower(firstString(raw, "token", "tokenAddress", "targetToken"))))
	_, _ = h.Write([]byte{':'})
	_, _ = h.Write([]byte(strings.ToLower(firstString(raw, "poolId", "poolID", "curve", "curveAddress", "marketAddress"))))
	return int(h.Sum32() % uint32(workers))
}

func (e *Engine) processForMonitors(ctx context.Context, ev Event) error {
	if err := e.refreshMonitorCache(ctx, false); err != nil {
		return err
	}
	e.monitorMu.RLock()
	users := make([]store.User, 0, len(e.monitorUsers))
	for _, u := range e.monitorUsers {
		users = append(users, u)
	}
	tokenSnapshot := make(map[int64][]store.Token, len(e.monitorTokens))
	for uid, tokens := range e.monitorTokens {
		tokenSnapshot[uid] = append([]store.Token(nil), tokens...)
	}
	e.monitorMu.RUnlock()
	for _, u := range users {
		tokens := tokenSnapshot[u.UserID]
		for _, t := range tokens {
			if (ev.CurveAddress != "" && strings.EqualFold(t.CurveAddress, ev.CurveAddress)) || (ev.PoolID != "" && strings.EqualFold(t.PoolID, ev.PoolID)) {
				t = e.enrichTokenSupply(ctx, t)
				copy := ev
				copy.UserID = u.UserID
				copy.TokenAddress = t.TokenAddress
				copy.CurveAddress = t.CurveAddress
				if t.Decimals != nil {
					copy.TokenDecimals = *t.Decimals
				}
				if t.QuoteDecimals != nil {
					copy.QuoteDecimals = *t.QuoteDecimals
				}
				if copy.MarketCap == 0 && t.MarketCap != nil {
					copy.MarketCap, _ = strconv.ParseFloat(*t.MarketCap, 64)
				}
				copy.Own = copy.Sender != "" && strings.EqualFold(copy.Sender, u.WalletAddress)
				if ev.PoolID != "" && (ev.Amount0Raw != "" || ev.Amount1Raw != "") {
					a0, _ := strconv.ParseFloat(ev.Amount0Raw, 64)
					a1, _ := strconv.ParseFloat(ev.Amount1Raw, 64)
					// V4 Swap amounts use the swapper's perspective: a positive
					// token delta means the token was paid out (buy), while a
					// negative token delta means it was sold into the pool.
					amt := a1
					quote := a0
					if strings.EqualFold(t.Currency0, t.TokenAddress) {
						amt, quote = a0, a1
					}
					if amt > 0 {
						copy.Side = "buy"
					} else if amt < 0 {
						copy.Side = "sell"
					}
					copy.QuoteAmountRaw = math.Abs(quote)
					copy.TokenAmountRaw = math.Abs(amt)
					copy.QuoteAmountText = absIntegerText(ev.Amount0Raw)
					copy.TokenAmountText = absIntegerText(ev.Amount1Raw)
					if strings.EqualFold(t.Currency0, t.TokenAddress) {
						copy.QuoteAmountText = absIntegerText(ev.Amount1Raw)
						copy.TokenAmountText = absIntegerText(ev.Amount0Raw)
					}
					if ev.SqrtPriceX96 != "" {
						if spot := v4SpotPrice(ev.SqrtPriceX96, strings.EqualFold(t.Currency0, t.TokenAddress)); spot > 0 {
							copy.PriceRaw = spot
						}
					}
					if copy.PriceRaw == 0 && copy.QuoteAmountRaw > 0 && copy.TokenAmountRaw > 0 {
						copy.PriceRaw = copy.QuoteAmountRaw / copy.TokenAmountRaw
					}
					if copy.Price == 0 && copy.PriceRaw > 0 {
						copy.Price = usdTokenPrice(copy.PriceRaw, t)
					}
					if marketCap := marketCapFromEvent(copy, t); marketCap > 0 {
						copy.MarketCap = marketCap
					}
					if copy.QuoteUSD == 0 && copy.QuoteAmountRaw > 0 {
						copy.QuoteUSD = rawQuoteUSD(copy.QuoteAmountRaw, t)
					}
					// Derive price impact from this swap's own execution price and
					// the PoolManager post-swap sqrt price. This deliberately does
					// not compare adjacent events, which is not a price-impact
					// measurement and caused false external-buy triggers.
					if copy.PriceImpactRatio == 0 && ev.SqrtPriceX96 != "" {
						copy.PriceImpactRatio = v4PriceImpact(ev.SqrtPriceX96, copy.QuoteAmountRaw, copy.TokenAmountRaw, strings.EqualFold(t.Currency0, t.TokenAddress))
						copy.Impact = copy.PriceImpactRatio
					}
					copy.PriceChangeRatio = copy.PriceImpactRatio
					_ = e.Process(ctx, copy)
				} else {
					if ev.SqrtPriceX96 != "" {
						if spot := v4SpotPrice(ev.SqrtPriceX96, strings.EqualFold(t.Currency0, t.TokenAddress)); spot > 0 {
							copy.PriceRaw = spot
						}
					}
					if copy.PriceRaw == 0 && copy.QuoteAmountRaw > 0 && copy.TokenAmountRaw > 0 {
						copy.PriceRaw = copy.QuoteAmountRaw / copy.TokenAmountRaw
					}
					if copy.Price == 0 && copy.PriceRaw > 0 {
						copy.Price = usdTokenPrice(copy.PriceRaw, t)
					}
					if marketCap := marketCapFromEvent(copy, t); marketCap > 0 {
						copy.MarketCap = marketCap
					}
					copy.PriceChangeRatio = copy.PriceImpactRatio
					_ = e.Process(ctx, copy)
				}
			}
		}
	}
	return nil
}

func (e *Engine) enrichTokenSupply(ctx context.Context, t store.Token) store.Token {
	if t.TotalSupplyRaw != nil && strings.TrimSpace(*t.TotalSupplyRaw) != "" {
		return t
	}
	key := strings.ToLower(strings.TrimSpace(t.TokenAddress))
	if key == "" || e.RPC == nil {
		return t
	}
	if cached, ok := e.supplies.Load(key); ok {
		if supply, ok := cached.(string); ok && supply != "" {
			t.TotalSupplyRaw = &supply
		}
		return t
	}
	supply, err := e.RPC.ERC20TotalSupply(ctx, t.TokenAddress)
	if err != nil || supply == "" {
		e.supplies.Store(key, "")
		return t
	}
	e.supplies.Store(key, supply)
	t.TotalSupplyRaw = &supply
	return t
}

func (e *Engine) refreshMonitorCache(ctx context.Context, force bool) error {
	e.monitorLoadMu.Lock()
	defer e.monitorLoadMu.Unlock()
	e.monitorMu.RLock()
	fresh := !e.monitorAt.IsZero() && time.Since(e.monitorAt) < 2*time.Second
	e.monitorMu.RUnlock()
	if fresh && !force {
		return nil
	}
	users, err := e.Store.Users(ctx)
	if err != nil {
		return err
	}
	userMap := make(map[int64]store.User, len(users))
	tokenMap := make(map[int64][]store.Token, len(users))
	for _, u := range users {
		userMap[u.UserID] = u
		tokens, tokenErr := e.Store.Tokens(ctx, u.UserID, nil)
		if tokenErr != nil {
			return tokenErr
		}
		tokenMap[u.UserID] = tokens
	}
	e.monitorMu.Lock()
	e.monitorUsers, e.monitorTokens, e.monitorAt = userMap, tokenMap, time.Now()
	e.monitorMu.Unlock()
	return nil
}

func (e *Engine) cachedUser(ctx context.Context, userID int64) (store.User, error) {
	_ = e.refreshMonitorCache(ctx, false)
	e.monitorMu.RLock()
	u, ok := e.monitorUsers[userID]
	e.monitorMu.RUnlock()
	if ok {
		return u, nil
	}
	return e.Store.User(ctx, userID)
}

func (e *Engine) cachedTokenEnabled(ctx context.Context, userID int64, address, curve string) (bool, error) {
	_ = e.refreshMonitorCache(ctx, false)
	e.monitorMu.RLock()
	for _, t := range e.monitorTokens[userID] {
		if strings.EqualFold(t.TokenAddress, address) && strings.EqualFold(t.CurveAddress, curve) {
			e.monitorMu.RUnlock()
			return t.Enabled, nil
		}
	}
	e.monitorMu.RUnlock()
	return e.Store.TokenEnabled(ctx, userID, address, curve)
}

func rawQuoteUSD(raw float64, t store.Token) float64 {
	decimals := 0
	if t.QuoteDecimals != nil {
		decimals = *t.QuoteDecimals
	}
	price := 0.0
	if t.QuoteUSDPrice != nil {
		price, _ = strconv.ParseFloat(*t.QuoteUSDPrice, 64)
	}
	if price == 0 {
		return 0
	}
	return raw / math.Pow10(decimals) * price
}

func usdTokenPrice(rawPrice float64, t store.Token) float64 {
	if rawPrice <= 0 {
		return 0
	}
	quoteDecimals, tokenDecimals := 0, 0
	if t.QuoteDecimals != nil {
		quoteDecimals = *t.QuoteDecimals
	}
	if t.Decimals != nil {
		tokenDecimals = *t.Decimals
	}
	quoteUSD := 0.0
	if t.QuoteUSDPrice != nil {
		quoteUSD, _ = strconv.ParseFloat(*t.QuoteUSDPrice, 64)
	}
	if quoteUSD <= 0 {
		return rawPrice
	}
	return rawPrice * math.Pow10(tokenDecimals-quoteDecimals) * quoteUSD
}

func v4SpotPrice(sqrt string, tokenIsCurrency0 bool) float64 {
	value := strings.TrimPrefix(strings.TrimPrefix(strings.TrimSpace(sqrt), "0x"), "0X")
	base := 10
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(sqrt)), "0x") {
		base = 16
	}
	s, ok := new(big.Int).SetString(value, base)
	if !ok || s.Sign() <= 0 {
		return 0
	}
	spot := new(big.Float).Quo(
		new(big.Float).Mul(new(big.Float).SetInt(s), new(big.Float).SetInt(s)),
		new(big.Float).SetInt(new(big.Int).Lsh(big.NewInt(1), 192)),
	)
	if !tokenIsCurrency0 {
		spot.Quo(big.NewFloat(1), spot)
	}
	result, _ := spot.Float64()
	if result <= 0 || math.IsNaN(result) || math.IsInf(result, 0) {
		return 0
	}
	return result
}

// marketCapFromEvent mirrors the Node strategy's marketCapUsdScaledRaw: use
// the current event price and total supply so a stale or missing persisted
// market_cap value cannot disable an otherwise valid strategy signal.
func marketCapFromEvent(ev Event, t store.Token) float64 {
	if t.TotalSupplyRaw == nil || strings.TrimSpace(*t.TotalSupplyRaw) == "" {
		return 0
	}
	rawPrice := ev.PriceRaw
	if rawPrice <= 0 && ev.QuoteAmountRaw > 0 && ev.TokenAmountRaw > 0 {
		rawPrice = ev.QuoteAmountRaw / ev.TokenAmountRaw
	}
	if rawPrice <= 0 {
		return 0
	}
	supply := new(big.Int)
	if _, ok := supply.SetString(integerText(*t.TotalSupplyRaw), 10); !ok || supply.Sign() <= 0 {
		return 0
	}
	quoteUSD := 0.0
	if t.QuoteUSDPrice != nil {
		quoteUSD, _ = strconv.ParseFloat(strings.TrimSpace(*t.QuoteUSDPrice), 64)
	}
	if quoteUSD <= 0 {
		quoteSymbol := ""
		if t.QuoteTokenSymbol != nil {
			quoteSymbol = strings.TrimSpace(*t.QuoteTokenSymbol)
		}
		switch strings.ToUpper(quoteSymbol) {
		case "USDG", "USDC", "USDT", "DAI":
			quoteUSD = 1
		}
	}
	if quoteUSD <= 0 {
		return 0
	}
	quoteDecimals := 18
	if t.QuoteDecimals != nil && *t.QuoteDecimals >= 0 {
		quoteDecimals = *t.QuoteDecimals
	}
	value := new(big.Float).SetInt(supply)
	value.Mul(value, big.NewFloat(rawPrice))
	value.Mul(value, big.NewFloat(quoteUSD))
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(quoteDecimals)), nil)
	value.Quo(value, new(big.Float).SetInt(scale))
	marketCap, _ := value.Float64()
	if marketCap <= 0 || math.IsNaN(marketCap) || math.IsInf(marketCap, 0) {
		return 0
	}
	return marketCap
}

func rawTokenPriceFromUSD(usdPrice float64, t store.Token) float64 {
	if usdPrice <= 0 {
		return 0
	}
	quoteDecimals, tokenDecimals := 0, 0
	if t.QuoteDecimals != nil {
		quoteDecimals = *t.QuoteDecimals
	}
	if t.Decimals != nil {
		tokenDecimals = *t.Decimals
	}
	quoteUSD := 0.0
	if t.QuoteUSDPrice != nil {
		quoteUSD, _ = strconv.ParseFloat(*t.QuoteUSDPrice, 64)
	}
	if quoteUSD <= 0 {
		return usdPrice
	}
	return usdPrice / (math.Pow10(tokenDecimals-quoteDecimals) * quoteUSD)
}

func v4PriceImpact(sqrt string, quoteRaw, tokenRaw float64, tokenIsCurrency0 bool) float64 {
	if quoteRaw <= 0 || tokenRaw <= 0 {
		return 0
	}
	value := strings.TrimPrefix(strings.TrimPrefix(sqrt, "0x"), "0X")
	base := 10
	if strings.HasPrefix(strings.ToLower(sqrt), "0x") {
		base = 16
	}
	s, ok := new(big.Int).SetString(value, base)
	if !ok {
		return 0
	}
	// sqrtPriceX96^2 / 2^192 is currency1 per currency0 in raw units.
	sf := new(big.Float).SetInt(s)
	spot := new(big.Float).Quo(new(big.Float).Mul(sf, sf), new(big.Float).SetInt(new(big.Int).Lsh(big.NewInt(1), 192)))
	spotF, _ := spot.Float64()
	if spotF <= 0 {
		return 0
	}
	exec := quoteRaw / tokenRaw
	if !tokenIsCurrency0 {
		spotF = 1 / spotF
	}
	if spotF <= 0 {
		return 0
	}
	// Match Node's signed estimate: startPrice ~= averageExecution^2 / final
	// spotPrice, then impact = final/start - 1.  Keeping the sign matters;
	// externalBuySell is intended to react to an upward price impact only.
	impact := (spotF*spotF)/(exec*exec) - 1
	if math.IsNaN(impact) || math.IsInf(impact, 0) {
		return 0
	}
	return impact
}

func decodeEvent(raw map[string]any) (Event, bool) {
	b, _ := json.Marshal(raw)
	var ev Event
	// Chain decoders intentionally keep raw amounts as decimal strings.  A
	// direct json.Unmarshal into float64 rejects those fields and silently
	// drops every WSS event, so use the struct for string fields and normalize
	// numeric aliases below instead of treating a type mismatch as a bad event.
	_ = json.Unmarshal(b, &ev)
	if ev.UserID == 0 {
		ev.UserID = int64(firstFloat(raw, "userId", "userID"))
	}
	if ev.Key == "" {
		ev.Key = firstString(raw, "eventKey", "sourceEventKey", "transactionHash", "txHash")
	}
	if ev.TokenAddress == "" {
		ev.TokenAddress = firstString(raw, "token", "tokenAddress", "targetToken")
	}
	if ev.CurveAddress == "" {
		ev.CurveAddress = firstString(raw, "curve", "curveAddress", "marketAddress")
	}
	if ev.Side == "" {
		ev.Side = strings.ToLower(firstString(raw, "type", "side", "direction"))
	}
	if ev.QuoteUSD == 0 {
		ev.QuoteUSD = firstFloat(raw, "quoteUsd", "quoteUSD", "amountUsd", "quoteAmountUSD")
	}
	if n := firstFloat(raw, "quoteAmountRaw", "amountRaw", "quoteInRaw", "quoteOutRaw"); n != 0 {
		ev.QuoteAmountRaw = n
	}
	ev.QuoteAmountText = firstString(raw, "quoteAmountRaw", "amountRaw", "quoteInRaw", "quoteOutRaw")
	if n := firstFloat(raw, "tokenAmountRaw", "tokensOutRaw", "tokensInRaw"); n != 0 {
		ev.TokenAmountRaw = n
	}
	ev.TokenAmountText = firstString(raw, "tokenAmountRaw", "tokensOutRaw", "tokensInRaw")
	if ev.Impact == 0 {
		ev.Impact = firstFloat(raw, "impact", "priceImpactRatio")
	}
	if ev.PriceImpactRatio == 0 {
		ev.PriceImpactRatio = firstFloat(raw, "priceImpactRatio", "impact")
	}
	if ev.PriceChangeRatio == 0 {
		ev.PriceChangeRatio = firstFloat(raw, "priceChangeRatio", "priceChange", "signedPriceChangeRatio")
	}
	if ev.Price == 0 {
		ev.Price = firstFloat(raw, "price", "tokenPriceUsd", "priceUsd")
	}
	if ev.PriceRaw == 0 {
		ev.PriceRaw = firstFloat(raw, "priceRaw")
	}
	if ev.TokenDecimals == 0 {
		ev.TokenDecimals = int(firstFloat(raw, "tokenDecimals", "decimals"))
	}
	if ev.QuoteDecimals == 0 {
		ev.QuoteDecimals = int(firstFloat(raw, "quoteDecimals"))
	}
	if ev.MarketCap == 0 {
		ev.MarketCap = firstFloat(raw, "marketCap", "marketCapUsd")
	}
	if ev.Block == "" {
		ev.Block = firstString(raw, "block", "blockNumber", "blockNumberHex")
	}
	if ev.Key == "" || (ev.TokenAddress == "" && ev.CurveAddress == "" && ev.PoolID == "") {
		return ev, false
	}
	return ev, true
}
func firstString(m map[string]any, keys ...string) string {
	for _, k := range keys {
		if v, ok := m[k]; ok && v != nil {
			s := stringValue(v)
			if s != "" && s != "<nil>" {
				return s
			}
		}
	}
	return ""
}

func stringValue(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case *string:
		if x != nil {
			return *x
		}
		return ""
	default:
		return fmt.Sprint(v)
	}
}

func absIntegerText(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	n := new(big.Int)
	if _, ok := n.SetString(value, 10); !ok {
		if strings.HasPrefix(strings.ToLower(value), "0x") {
			if _, ok := n.SetString(value[2:], 16); !ok {
				return ""
			}
		} else {
			return ""
		}
	}
	return n.Abs(n).String()
}

func integerText(value string) string {
	if value := absIntegerText(value); value != "" {
		return value
	}
	return "0"
}

func rawFloat(value string) float64 {
	if value == "" {
		return 0
	}
	n, ok := new(big.Float).SetString(value)
	if !ok {
		return 0
	}
	f, _ := n.Float64()
	return f
}

func decimalScaled(value string, decimals int) (*big.Int, bool) {
	value = strings.TrimSpace(value)
	if value == "" || decimals < 0 {
		return nil, false
	}
	parts := strings.Split(value, ".")
	if len(parts) > 2 || parts[0] == "" {
		return nil, false
	}
	whole := new(big.Int)
	if _, ok := whole.SetString(parts[0], 10); !ok || whole.Sign() < 0 {
		return nil, false
	}
	fraction := ""
	if len(parts) == 2 {
		fraction = parts[1]
	}
	if len(fraction) > decimals {
		fraction = fraction[:decimals]
	}
	fraction = fraction + strings.Repeat("0", decimals-len(fraction))
	frac := new(big.Int)
	if fraction != "" {
		if _, ok := frac.SetString(fraction, 10); !ok {
			return nil, false
		}
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	return new(big.Int).Add(new(big.Int).Mul(whole, scale), frac), true
}

func quoteToNativeRaw(quoteRaw string, quoteDecimals int, quotePriceEth string) string {
	quote := new(big.Int)
	if _, ok := quote.SetString(integerText(quoteRaw), 10); !ok || quote.Sign() <= 0 {
		return "0"
	}
	price, ok := decimalScaled(quotePriceEth, 18)
	if !ok || price.Sign() <= 0 {
		return "0"
	}
	quoteScale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(quoteDecimals)), nil)
	return new(big.Int).Quo(new(big.Int).Mul(quote, price), quoteScale).String()
}

func quotePriceEthFromToken(t store.Token, nativeUSDPrice string) string {
	quote := strings.ToLower(strings.TrimSpace(t.QuoteTokenAddress))
	if quote == chain.NativeAddress || quote == chain.WrappedNativeAddress {
		return "1"
	}
	quoteUSD := 0.0
	if t.QuoteUSDPrice != nil {
		quoteUSD, _ = strconv.ParseFloat(strings.TrimSpace(*t.QuoteUSDPrice), 64)
	}
	if quoteUSD <= 0 && t.QuoteTokenSymbol != nil {
		switch strings.ToUpper(strings.TrimSpace(*t.QuoteTokenSymbol)) {
		case "USDG", "USDC", "USDT", "DAI":
			quoteUSD = 1
		}
	}
	if quoteUSD > 0 && t.TokenPriceEth != nil && t.TokenPriceUsd != nil {
		tokenPriceEth, _ := strconv.ParseFloat(strings.TrimSpace(*t.TokenPriceEth), 64)
		tokenPriceUSD, _ := strconv.ParseFloat(strings.TrimSpace(*t.TokenPriceUsd), 64)
		if tokenPriceEth > 0 && tokenPriceUSD > 0 {
			return strconv.FormatFloat((tokenPriceEth/tokenPriceUSD)*quoteUSD, 'f', -1, 64)
		}
	}
	nativeUSD, _ := strconv.ParseFloat(strings.TrimSpace(nativeUSDPrice), 64)
	if quoteUSD <= 0 || nativeUSD <= 0 {
		return ""
	}
	return strconv.FormatFloat(quoteUSD/nativeUSD, 'f', -1, 64)
}

func nativeRouteMinimumOutput(ev Event, amountRaw string, quoteDecimals int, quotePriceEth string, slippage float64, hops []any) string {
	amount := new(big.Int)
	if _, ok := amount.SetString(integerText(amountRaw), 10); !ok || amount.Sign() <= 0 {
		return "0"
	}
	priceEth, ok := decimalScaled(quotePriceEth, 18)
	if !ok || priceEth.Sign() <= 0 || ev.QuoteAmountText == "" || ev.TokenAmountText == "" {
		return "0"
	}
	quoteScale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(quoteDecimals)), nil)
	quoteRaw := new(big.Int).Quo(new(big.Int).Mul(amount, quoteScale), priceEth)
	quoteEvent := new(big.Int)
	tokenEvent := new(big.Int)
	if _, ok := quoteEvent.SetString(integerText(ev.QuoteAmountText), 10); !ok || quoteEvent.Sign() <= 0 {
		return "0"
	}
	if _, ok := tokenEvent.SetString(integerText(ev.TokenAmountText), 10); !ok || tokenEvent.Sign() <= 0 {
		return "0"
	}
	expected := new(big.Int).Quo(new(big.Int).Mul(quoteRaw, tokenEvent), quoteEvent)
	for _, raw := range hops {
		h, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		fee := chain.ToBig(h["fee"])
		if fee.Sign() < 0 || fee.Cmp(big.NewInt(1_000_000)) >= 0 {
			return "0"
		}
		expected.Mul(expected, new(big.Int).Sub(big.NewInt(1_000_000), fee))
		expected.Quo(expected, big.NewInt(1_000_000))
	}
	if expected.Sign() <= 0 {
		return "0"
	}
	slipBps := int64(math.Round(slippage * 10_000))
	if slipBps < 0 {
		slipBps = 0
	}
	if slipBps > 10_000 {
		slipBps = 10_000
	}
	minimum := new(big.Int).Mul(expected, big.NewInt(10_000-slipBps))
	minimum.Quo(minimum, big.NewInt(10_000))
	if minimum.Sign() <= 0 {
		return "1"
	}
	return minimum.String()
}

func rawRatio(raw string, ratio float64) string {
	if raw == "" || ratio <= 0 || ratio > 1 {
		return "0"
	}
	n := new(big.Int)
	if _, ok := n.SetString(integerText(raw), 10); !ok || n.Sign() <= 0 {
		return "0"
	}
	r, ok := new(big.Rat).SetString(strconv.FormatFloat(ratio, 'f', 18, 64))
	if !ok || r.Sign() <= 0 {
		return "0"
	}
	out := new(big.Rat).Mul(new(big.Rat).SetInt(n), r)
	result := new(big.Int).Quo(out.Num(), out.Denom())
	if result.Sign() <= 0 {
		return "0"
	}
	return result.String()
}

func rawQuoteRatio(quoteRaw string, rawPrice, ratio float64) string {
	if quoteRaw == "" || rawPrice <= 0 || ratio <= 0 || ratio > 1 {
		return "0"
	}
	quote := new(big.Float)
	if _, ok := quote.SetString(integerText(quoteRaw)); !ok {
		return "0"
	}
	value := new(big.Float).Quo(quote, new(big.Float).SetFloat64(rawPrice))
	value.Mul(value, new(big.Float).SetFloat64(ratio))
	out, _ := value.Int(nil)
	if out == nil || out.Sign() <= 0 {
		return "0"
	}
	return out.String()
}

func firstFloat(m map[string]any, keys ...string) float64 {
	for _, k := range keys {
		if v, ok := m[k]; ok {
			switch x := v.(type) {
			case float64:
				return x
			case float32:
				return float64(x)
			case int:
				return float64(x)
			case int64:
				return float64(x)
			case uint64:
				return float64(x)
			case json.Number:
				f, _ := strconv.ParseFloat(string(x), 64)
				return f
			case string:
				f, _ := strconv.ParseFloat(x, 64)
				return f
			}
		}
	}
	return 0
}

func (e *Engine) Process(ctx context.Context, ev Event) error {
	// Price change is the current swap's signed impact. It must not depend on
	// a previous event observed by this process.
	ev.PriceChangeRatio = ev.PriceImpactRatio
	key := fmt.Sprintf("%d:%s:%s", ev.UserID, strings.ToLower(ev.TokenAddress), strings.ToLower(ev.CurveAddress))
	muI, _ := e.locks.LoadOrStore(key, &sync.Mutex{})
	mu := muI.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()
	u, err := e.cachedUser(ctx, ev.UserID)
	if err != nil {
		return err
	}
	cfg := Parse(u.Config)
	if !u.Enabled || !cfg.Enabled {
		return nil
	}
	if enabled, err := e.cachedTokenEnabled(ctx, ev.UserID, ev.TokenAddress, ev.CurveAddress); err != nil || !enabled {
		return err
	}
	e.devInfo("监听到匹配 token 的交易",
		zap.Int64("userID", ev.UserID),
		zap.String("eventKey", ev.Key),
		zap.String("side", strings.ToLower(ev.Side)),
		zap.String("token", ev.TokenAddress),
		zap.String("curve", ev.CurveAddress),
		zap.String("pool", ev.PoolID),
		zap.Float64("quoteUSD", ev.QuoteUSD),
		zap.Float64("price", ev.Price),
	)
	if cfg.MinMCP > 0 && ev.MarketCap <= 0 {
		e.devInfo("策略跳过交易",
			zap.Int64("userID", ev.UserID),
			zap.String("reason", "market_cap_missing"),
			zap.Float64("minMCP", cfg.MinMCP),
			zap.Float64("marketCap", ev.MarketCap),
			zap.String("token", ev.TokenAddress),
		)
		return nil
	}
	if ev.MarketCap > 0 && ev.MarketCap < cfg.MinMCP {
		e.devInfo("策略跳过交易",
			zap.Int64("userID", ev.UserID),
			zap.String("reason", "market_cap_below_minimum"),
			zap.Float64("minMCP", cfg.MinMCP),
			zap.Float64("marketCap", ev.MarketCap),
			zap.String("token", ev.TokenAddress),
		)
		return nil
	}
	rule, ruleIndex, ok := cfg.RuleWithIndex(ev.MarketCap)
	if !ok {
		e.devInfo("策略跳过交易",
			zap.Int64("userID", ev.UserID),
			zap.String("reason", "no_token_config_rule"),
			zap.Float64("marketCap", ev.MarketCap),
			zap.String("token", ev.TokenAddress),
		)
		return nil
	}
	e.devInfo("交易匹配策略配置",
		zap.Int64("userID", ev.UserID),
		zap.String("configName", cfg.Name),
		zap.Int("ruleIndex", ruleIndex),
		zap.Float64("ruleMaxMCP", rule.MaxMCP),
		zap.Float64("marketCap", ev.MarketCap),
		zap.String("token", ev.TokenAddress),
		zap.String("curve", ev.CurveAddress),
	)
	p, err := e.loadPosition(ctx, ev)
	if err != nil {
		return err
	}
	e.devInfo("策略评估输入",
		zap.Int64("userID", ev.UserID),
		zap.String("side", strings.ToLower(ev.Side)),
		zap.Float64("priceImpactRatio", ev.PriceImpactRatio),
		zap.Float64("impact", ev.Impact),
		zap.Float64("priceChangeRatio", ev.PriceChangeRatio),
		zap.Float64("minSellRatio", rule.MinSellRatio),
		zap.Float64("positionAmount", p.Amount),
		zap.String("positionAmountRaw", p.AmountRaw),
		zap.Int("buyCount", p.BuyCount),
		zap.String("token", ev.TokenAddress),
	)
	if ev.Own {
		if inserted, markErr := e.markStrategyEvent(ctx, ev); markErr != nil || !inserted {
			return markErr
		}
		return e.applyOwnFill(ctx, ev, p, cfg)
	}
	// A position may receive several market events while a previous sell is
	// still waiting for its receipt. loadPosition includes the durable pending
	// flag in the same round trip, so a later event cannot broadcast a second
	// sell without adding another database latency hop to the WSS path.
	if p.PendingSell {
		return nil
	}
	action, amount, reason := evaluate(cfg, rule, ev, p)
	if action == "" || amount <= 0 {
		e.devInfo("策略未触发交易",
			zap.Int64("userID", ev.UserID),
			zap.String("reason", "conditions_not_met"),
			zap.String("side", strings.ToLower(ev.Side)),
			zap.Float64("amount", amount),
			zap.String("token", ev.TokenAddress),
		)
		return nil
	}
	if action != "" && amount > 0 {
		e.devInfo("策略决定执行交易",
			zap.Int64("userID", ev.UserID),
			zap.String("configName", cfg.Name),
			zap.String("action", action),
			zap.String("reason", reason),
			zap.Float64("amount", amount),
			zap.String("token", ev.TokenAddress),
			zap.String("curve", ev.CurveAddress),
		)
		if action == "sell" && reason == "external_buy_signal" && cfg.ExternalBuySell.BuyAmountRatio > 0 {
			rawEventToken := ev.TokenAmountText
			if rawEventToken == "" {
				rawEventToken = integerRaw(ev.TokenAmountRaw)
			}
			candidate := "0"
			if ev.QuoteAmountText != "" && ev.PriceRaw > 0 {
				candidate = rawQuoteRatio(ev.QuoteAmountText, ev.PriceRaw, cfg.ExternalBuySell.BuyAmountRatio)
			}
			if candidate == "0" {
				candidate = rawRatio(rawEventToken, cfg.ExternalBuySell.BuyAmountRatio)
			}
			if candidate != "0" {
				ev.ExecutionAmountRaw = candidate
			}
			if balance, ok := new(big.Int).SetString(integerText(p.AmountRaw), 10); ok && ev.ExecutionAmountRaw != "" {
				if requested, ok := new(big.Int).SetString(integerText(ev.ExecutionAmountRaw), 10); ok && requested.Cmp(balance) > 0 {
					ev.ExecutionAmountRaw = balance.String()
				}
			}
		}
		if inserted, markErr := e.markStrategyEvent(ctx, ev); markErr != nil || !inserted {
			return markErr
		}
		if action == "buy" {
			// Pending replacement is a V4-only path. Legacy Pons buys keep the
			// original one-shot transaction behavior even when the strategy flag
			// is enabled.
			accepted, pe := e.persistPendingBuy(ctx, ev, amount, cfg.ReplacePending && ev.PoolID != "", cfg.PendingBuyTimeoutSecond)
			if pe != nil {
				e.devInfo("交易未执行",
					zap.Error(pe),
					zap.String("reason", "persist_pending_buy_failed"),
					zap.String("action", action),
					zap.String("token", ev.TokenAddress),
				)
				return pe
			}
			if !accepted {
				e.devInfo("交易未执行",
					zap.String("reason", "pending_buy_not_accepted"),
					zap.Bool("replacePending", cfg.ReplacePending),
					zap.Int("pendingBuyTimeoutSecond", cfg.PendingBuyTimeoutSecond),
					zap.String("token", ev.TokenAddress),
				)
				return nil
			}
		}
		if reason == "external_buy_signal" && cfg.ExternalBuySell.CooldownSecond > 0 {
			until := time.Now().Add(time.Duration(cfg.ExternalBuySell.CooldownSecond) * time.Second)
			p.CooldownUntil = &until
			if err := e.savePosition(ctx, ev, p); err != nil {
				return err
			}
		}
		var result map[string]any
		if e.ExecuteAction != nil {
			var execErr error
			result, execErr = e.ExecuteAction(ctx, ev, action, amount)
			if execErr != nil {
				// Keep the pending buy row for reconciliation/replacement and do
				// not claim a successful sell before its receipt is confirmed.
				e.devInfo("交易执行失败",
					zap.Error(execErr),
					zap.String("action", action),
					zap.String("reason", reason),
					zap.String("token", ev.TokenAddress),
				)
				return execErr
			}
		} else if e.Trading != nil {
			var execErr error
			result, execErr = e.executeLive(ctx, ev, action, amount, cfg)
			if execErr != nil {
				e.devInfo("交易执行失败",
					zap.Error(execErr),
					zap.String("action", action),
					zap.String("reason", reason),
					zap.String("token", ev.TokenAddress),
				)
				return execErr
			}
		} else {
			e.devInfo("交易未执行",
				zap.String("reason", "trading_not_configured"),
				zap.String("action", action),
				zap.String("token", ev.TokenAddress),
			)
			return nil
		}
		e.devInfo("交易执行结果",
			zap.String("action", action),
			zap.String("mode", strings.ToLower(fmt.Sprint(result["mode"]))),
			zap.String("transactionHash", firstString(result, "transactionHash", "hash")),
			zap.String("token", ev.TokenAddress),
		)
		// Live broadcasts are intents. The position and sell counters are
		// updated only by applyOwnFill after a confirmed chain event.
		if hash := firstString(result, "transactionHash", "hash"); hash != "" {
			if err := e.savePendingAction(ctx, hash, ev, action, amount, reason); err != nil {
				return err
			}
		}
		return nil
	}
	return nil
}

func (e *Engine) markStrategyEvent(ctx context.Context, ev Event) (bool, error) {
	eventKey := fmt.Sprintf("%d:%s", ev.UserID, ev.Key)
	var inserted bool
	err := e.Store.DB.QueryRow(ctx, `INSERT INTO strategy_events(source_event_key,user_id,token_address,curve_address,created_at) VALUES($1,$2,$3,$4,now()) ON CONFLICT(source_event_key) DO NOTHING RETURNING true`, eventKey, ev.UserID, ev.TokenAddress, ev.CurveAddress).Scan(&inserted)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "no rows") {
		return false, nil
	}
	return inserted, err
}

func integerRaw(v float64) string {
	if v <= 0 {
		return "0"
	}
	f := new(big.Float).SetFloat64(v)
	n, _ := f.Int(nil)
	return n.String()
}

func amountAtRawPrice(amount string, rawPrice, slippage float64, buy bool) string {
	if amount == "" || rawPrice <= 0 || slippage < 0 || slippage >= 1 {
		return "0"
	}
	n, ok := new(big.Float).SetString(amount)
	if !ok {
		return "0"
	}
	price := new(big.Float).SetFloat64(rawPrice)
	if buy {
		n.Quo(n, price)
	} else {
		n.Mul(n, price)
	}
	n.Mul(n, new(big.Float).SetFloat64(1-slippage))
	out, _ := n.Int(nil)
	if out == nil || out.Sign() <= 0 {
		return "0"
	}
	return out.String()
}

// amountAtRawRatio performs the same minimum-output calculation with the
// exact integer quote/token amounts carried by a chain event. It avoids first
// converting a 18-decimal raw amount to float64 and is used when the event
// contains its original decimal strings.
func amountAtRawRatio(amount, quoteRaw, tokenRaw string, slippage float64, buy bool) string {
	if amount == "" || quoteRaw == "" || tokenRaw == "" || slippage < 0 || slippage >= 1 {
		return "0"
	}
	n, ok1 := new(big.Int).SetString(integerText(amount), 10)
	q, ok2 := new(big.Int).SetString(integerText(quoteRaw), 10)
	t, ok3 := new(big.Int).SetString(integerText(tokenRaw), 10)
	if !ok1 || !ok2 || !ok3 || q.Sign() <= 0 || t.Sign() <= 0 {
		return "0"
	}
	// Convert the configured decimal ratio through a rational value so the
	// floor operation remains deterministic.
	slipText := strconv.FormatFloat(1-slippage, 'f', 18, 64)
	slipRat, ok := new(big.Rat).SetString(slipText)
	if !ok || slipRat.Sign() <= 0 {
		return "0"
	}
	out := new(big.Rat).SetInt(n)
	if buy {
		out.Mul(out, new(big.Rat).SetInt(t))
		out.Quo(out, new(big.Rat).SetInt(q))
	} else {
		out.Mul(out, new(big.Rat).SetInt(q))
		out.Quo(out, new(big.Rat).SetInt(t))
	}
	out.Mul(out, slipRat)
	result := new(big.Int).Quo(out.Num(), out.Denom())
	if result.Sign() <= 0 {
		return "0"
	}
	return result.String()
}

// executeLive is the default strategy executor. It deliberately broadcasts
// through the same Trading implementation exposed by the HTTP API; receipt
// events remain the single source of truth for position accounting.
func (e *Engine) executeLive(ctx context.Context, ev Event, side string, amount float64, cfg Config) (map[string]any, error) {
	u, err := e.Store.User(ctx, ev.UserID)
	if err != nil {
		return nil, err
	}
	if u.PrivateKeyEncrypted == nil {
		return nil, fmt.Errorf("monitor user private key not found")
	}
	key, err := secret.Decrypt(*u.PrivateKeyEncrypted, e.EncryptionKey)
	if err != nil {
		return nil, err
	}
	t, err := e.Store.Token(ctx, ev.UserID, ev.TokenAddress, ev.CurveAddress)
	if err != nil {
		return nil, err
	}
	// Prefer a validated cached V4 route. Route input is raw quote/token units;
	// strategy buy amounts are USD, so convert using the event's quote price.
	routes, err := e.Store.Routes(ctx, ev.UserID)
	if err != nil {
		return nil, err
	}
	direction := strings.ToLower(side)
	for _, route := range routes {
		if !strings.EqualFold(stringValue(route["direction"]), direction) {
			continue
		}
		if target := stringValue(route["targetToken"]); target != "" && !strings.EqualFold(target, ev.TokenAddress) {
			continue
		}
		if ev.PoolID != "" && stringValue(route["targetPoolId"]) != "" && !strings.EqualFold(stringValue(route["targetPoolId"]), ev.PoolID) {
			continue
		}
		hops, ok := route["hops"].([]any)
		if !ok || len(hops) == 0 {
			continue
		}
		amountRaw := integerRaw(amount)
		first := stringValue(route["sourceToken"])
		destination := stringValue(route["destinationToken"])
		targetQuotePriceEth := stringValue(route["targetQuotePriceEth"])
		if targetQuotePriceEth == "" {
			targetQuotePriceEth = quotePriceEthFromToken(t, e.NativeUSDPrice)
		}
		quoteDecimals := 18
		if t.QuoteDecimals != nil && *t.QuoteDecimals >= 0 {
			quoteDecimals = *t.QuoteDecimals
		}
		if side == "sell" && ev.ExecutionAmountRaw != "" {
			amountRaw = integerText(ev.ExecutionAmountRaw)
		}
		if side == "buy" {
			usdPerRaw := 0.0
			if t.QuoteUSDPrice != nil && t.QuoteDecimals != nil {
				usdPerRaw, _ = strconv.ParseFloat(*t.QuoteUSDPrice, 64)
				usdPerRaw /= math.Pow10(*t.QuoteDecimals)
			} else if ev.QuoteUSD > 0 && ev.QuoteAmountRaw > 0 {
				usdPerRaw = ev.QuoteUSD / ev.QuoteAmountRaw
			}
			if usdPerRaw > 0 {
				amountRaw = integerRaw(amount / usdPerRaw)
			}
			quoteIsNative := strings.EqualFold(destination, chain.NativeAddress) || strings.EqualFold(destination, chain.WrappedNativeAddress)
			if strings.EqualFold(first, chain.NativeAddress) && !quoteIsNative {
				if targetQuotePriceEth == "" {
					return nil, fmt.Errorf("route target quote ETH price is missing")
				}
				amountRaw = quoteToNativeRaw(amountRaw, quoteDecimals, targetQuotePriceEth)
				if amountRaw == "0" {
					return nil, fmt.Errorf("route target quote ETH price is invalid")
				}
			}
		}
		path := make([]any, 0, len(hops))
		for _, raw := range hops {
			h, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			intermediate := h["intermediateCurrency"]
			if intermediate == nil {
				intermediate = h["tokenOut"]
			}
			path = append(path, map[string]any{"intermediateCurrency": intermediate, "fee": h["fee"], "tickSpacing": h["tickSpacing"], "hooks": h["hooks"], "hookData": h["hookData"]})
		}
		if amountRaw == "0" {
			continue
		}
		minOut := "0"
		rawPrice := ev.PriceRaw
		if rawPrice <= 0 && ev.QuoteAmountRaw > 0 && ev.TokenAmountRaw > 0 {
			rawPrice = ev.QuoteAmountRaw / ev.TokenAmountRaw
		}
		if side == "buy" && cfg.Slippage >= 0 && cfg.Slippage < 1 {
			quoteIsNative := strings.EqualFold(destination, chain.NativeAddress) || strings.EqualFold(destination, chain.WrappedNativeAddress)
			if strings.EqualFold(first, chain.NativeAddress) && !quoteIsNative && targetQuotePriceEth != "" {
				minOut = nativeRouteMinimumOutput(ev, amountRaw, quoteDecimals, targetQuotePriceEth, cfg.Slippage, hops)
			}
			if minOut == "0" && (quoteIsNative || targetQuotePriceEth == "") && rawPrice > 0 && ev.QuoteAmountText != "" && ev.TokenAmountText != "" {
				minOut = amountAtRawRatio(amountRaw, ev.QuoteAmountText, ev.TokenAmountText, cfg.Slippage, true)
			} else if minOut == "0" && (quoteIsNative || targetQuotePriceEth == "") && rawPrice > 0 {
				minOut = amountAtRawPrice(amountRaw, rawPrice, cfg.Slippage, true)
			}
		}
		e.devInfo("交易执行参数",
			zap.String("action", side),
			zap.String("amountInRaw", amountRaw),
			zap.String("amountOutMinimumRaw", minOut),
			zap.String("targetQuotePriceEth", targetQuotePriceEth),
			zap.Int("quoteDecimals", quoteDecimals),
			zap.String("token", ev.TokenAddress),
		)
		req := map[string]any{"currencyIn": first, "path": path, "amountInRaw": amountRaw, "amountOutMinimumRaw": minOut, "recipient": u.WalletAddress, "customRecipient": false, "privateKey": key, "wrapNative": side == "buy" && strings.EqualFold(first, chain.NativeAddress), "unwrapNative": side == "sell" && strings.EqualFold(destination, chain.NativeAddress)}
		opts := chain.BroadcastOptions{}
		if side == "buy" {
			var pendingNonce *int64
			var previousFee, previousTip *string
			if qe := e.Store.DB.QueryRow(ctx, `SELECT nonce,max_fee_per_gas::text,max_priority_fee_per_gas::text FROM strategy_pending_buys WHERE user_id=$1 AND token_address=$2 AND curve_address=$3 AND status='pending'`, ev.UserID, ev.TokenAddress, ev.CurveAddress).Scan(&pendingNonce, &previousFee, &previousTip); qe == nil && pendingNonce != nil {
				n := uint64(*pendingNonce)
				opts.Nonce = &n
				opts.FeeBumpBps = 1250
				if previousFee != nil {
					opts.MaxFeePerGas = chain.ToBig(*previousFee)
				}
				if previousTip != nil {
					opts.TipPerGas = chain.ToBig(*previousTip)
				}
			}
		}
		broadcast, be := e.Trading.BroadcastV4(ctx, req, opts)
		if be != nil {
			return nil, be
		}
		if side == "buy" {
			maxFee, tip := "", ""
			if broadcast.MaxFeePerGas != nil {
				maxFee = broadcast.MaxFeePerGas.String()
			}
			if broadcast.TipPerGas != nil {
				tip = broadcast.TipPerGas.String()
			}
			if err := e.UpdatePendingBroadcast(ctx, ev.UserID, ev.TokenAddress, ev.CurveAddress, broadcast.TransactionHash, broadcast.Nonce, maxFee, tip, minOut); err != nil {
				// The RPC already accepted the transaction.  Returning the database
				// error keeps the position lock/error visible to the caller so the
				// pending row can be reconciled instead of falsely treating the
				// broadcast as accounted for.
				return nil, err
			}
		}
		return map[string]any{"mode": "broadcasted", "transactionHash": broadcast.TransactionHash, "hash": broadcast.TransactionHash, "nonce": broadcast.Nonce, "from": broadcast.From}, nil
	}
	// Pons V2 fallback. Buy amounts are USD and are converted to curve quote
	// units when AVE supplied a quote price; sells already use token raw units.
	amountRaw := integerRaw(amount)
	if side == "sell" && ev.ExecutionAmountRaw != "" {
		amountRaw = integerText(ev.ExecutionAmountRaw)
	}
	if side == "buy" && ev.QuoteUSD > 0 && ev.QuoteAmountRaw > 0 {
		amountRaw = integerRaw(amount * ev.QuoteAmountRaw / ev.QuoteUSD)
	}
	return e.Trading.PonsSwap(ctx, map[string]any{"curveAddress": t.CurveAddress, "tokenAddress": t.TokenAddress, "recipient": u.WalletAddress, "privateKey": key, "amountRaw": amountRaw, "slippageBps": integerRaw(cfg.Slippage * 10000)}, side == "buy")
}

// persistPendingBuy accepts an existing pending buy for fee-bumped replacement
// after the configured timeout. The original hash remains in buy_attempts so
// receipt reconciliation can still classify it if it eventually lands.
func (e *Engine) persistPendingBuy(ctx context.Context, ev Event, amount float64, replace bool, timeoutSecond int) (bool, error) {
	var accepted bool
	if replace {
		err := e.Store.DB.QueryRow(ctx, `INSERT INTO strategy_pending_buys(user_id,token_address,curve_address,quote_usd,source_event_key,replacement_count,status) VALUES($1,$2,$3,$4,$5,0,'pending') ON CONFLICT(user_id,token_address,curve_address) DO UPDATE SET quote_usd=EXCLUDED.quote_usd,source_event_key=EXCLUDED.source_event_key,transaction_hash=CASE WHEN strategy_pending_buys.status='pending' THEN strategy_pending_buys.transaction_hash ELSE NULL END,nonce=CASE WHEN strategy_pending_buys.status='pending' THEN strategy_pending_buys.nonce ELSE NULL END,max_fee_per_gas=CASE WHEN strategy_pending_buys.status='pending' THEN strategy_pending_buys.max_fee_per_gas ELSE NULL END,max_priority_fee_per_gas=CASE WHEN strategy_pending_buys.status='pending' THEN strategy_pending_buys.max_priority_fee_per_gas ELSE NULL END,amount_out_minimum_raw=CASE WHEN strategy_pending_buys.status='pending' THEN strategy_pending_buys.amount_out_minimum_raw ELSE NULL END,winner_transaction_hash=NULL,replacement_count=CASE WHEN strategy_pending_buys.status='pending' THEN strategy_pending_buys.replacement_count+1 ELSE 0 END,status='pending',updated_at=now() WHERE strategy_pending_buys.status IN ('reverted','failed','expired') OR (strategy_pending_buys.status='pending' AND $6 > 0 AND strategy_pending_buys.updated_at < now() - ($6 * interval '1 second')) RETURNING true`, ev.UserID, ev.TokenAddress, ev.CurveAddress, amount, ev.Key, timeoutSecond).Scan(&accepted)
		if err != nil && strings.Contains(strings.ToLower(err.Error()), "no rows") {
			return false, nil
		}
		return accepted, err
	}
	err := e.Store.DB.QueryRow(ctx, `INSERT INTO strategy_pending_buys(user_id,token_address,curve_address,quote_usd,source_event_key,status) VALUES($1,$2,$3,$4,$5,'pending') ON CONFLICT(user_id,token_address,curve_address) DO UPDATE SET quote_usd=EXCLUDED.quote_usd,source_event_key=EXCLUDED.source_event_key,transaction_hash=NULL,nonce=NULL,max_fee_per_gas=NULL,max_priority_fee_per_gas=NULL,amount_out_minimum_raw=NULL,winner_transaction_hash=NULL,replacement_count=0,status='pending',updated_at=now() WHERE strategy_pending_buys.status IN ('reverted','failed','expired') OR (strategy_pending_buys.status='pending' AND $6 > 0 AND strategy_pending_buys.updated_at < now() - ($6 * interval '1 second')) RETURNING true`, ev.UserID, ev.TokenAddress, ev.CurveAddress, amount, ev.Key, timeoutSecond).Scan(&accepted)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "no rows") {
		return false, nil
	}
	return accepted, err
}

// UpdatePendingBroadcast records the transaction identity after RPC accepts a
// buy. It is intentionally separate from persistPendingBuy: the strategy can
// accept a signal before the signing/broadcast phase, and a replacement keeps
// the same nonce while updating only the latest hash and fee caps.
func (e *Engine) UpdatePendingBroadcast(ctx context.Context, userID int64, token, curve, hash string, nonce uint64, maxFee, tip, amountOutMinimum string) error {
	var quoteUSD float64
	var sourceKey string
	if err := e.Store.DB.QueryRow(ctx, `SELECT quote_usd::double precision,source_event_key FROM strategy_pending_buys WHERE user_id=$1 AND token_address=$2 AND curve_address=$3 AND status='pending'`, userID, token, curve).Scan(&quoteUSD, &sourceKey); err != nil {
		return err
	}
	if _, err := e.Store.DB.Exec(ctx, `UPDATE strategy_pending_buys SET transaction_hash=$4,nonce=$5,max_fee_per_gas=NULLIF($6,'')::numeric,max_priority_fee_per_gas=NULLIF($7,'')::numeric,amount_out_minimum_raw=NULLIF($8,''),updated_at=now() WHERE user_id=$1 AND token_address=$2 AND curve_address=$3 AND status='pending'`, userID, token, curve, hash, nonce, maxFee, tip, amountOutMinimum); err != nil {
		return err
	}
	_, err := e.Store.DB.Exec(ctx, `INSERT INTO strategy_buy_attempts(user_id,token_address,curve_address,transaction_hash,nonce,quote_usd,source_event_key,amount_out_minimum_raw,status,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,NULLIF($8,''),'pending',now()) ON CONFLICT(transaction_hash) DO UPDATE SET nonce=EXCLUDED.nonce,quote_usd=EXCLUDED.quote_usd,source_event_key=EXCLUDED.source_event_key,amount_out_minimum_raw=EXCLUDED.amount_out_minimum_raw,updated_at=now()`, userID, token, curve, hash, nonce, quoteUSD, sourceKey, amountOutMinimum)
	return err
}

// MarkPendingWinner closes a pending buy only after the corresponding receipt
// has been confirmed and accounting has succeeded. A reverted or timed-out
// transaction remains pending for reconciliation/retry.
func (e *Engine) MarkPendingWinner(ctx context.Context, userID int64, token, curve, hash string) error {
	_, err := e.Store.DB.Exec(ctx, `UPDATE strategy_pending_buys SET status='confirmed',winner_transaction_hash=$4,updated_at=now() WHERE user_id=$1 AND token_address=$2 AND curve_address=$3 AND status IN ('pending','confirmed_pending_accounting')`, userID, token, curve, hash)
	if err == nil {
		_, err = e.Store.DB.Exec(ctx, `UPDATE strategy_buy_attempts SET status=CASE WHEN transaction_hash=$4 THEN 'confirmed' ELSE 'replaced' END,updated_at=now() WHERE user_id=$1 AND token_address=$2 AND curve_address=$3 AND status='pending'`, userID, token, curve, hash)
	}
	return err
}

func (e *Engine) loadPosition(ctx context.Context, ev Event) (position, error) {
	var p position
	var amountRaw string
	var first, next, cool, last *time.Time
	err := e.Store.DB.QueryRow(ctx, `SELECT token_amount_raw::text,average_cost_usd::double precision,first_buy_price::double precision,last_buy_price::double precision,buy_count,sell_count,profit_sell_level,first_bought_at,next_scheduled_sell_at,external_cooldown_until,last_buy_at,EXISTS(SELECT 1 FROM strategy_pending_actions a WHERE a.user_id=strategy_positions.user_id AND a.token_address=strategy_positions.token_address AND a.curve_address=strategy_positions.curve_address AND a.side='sell' AND a.status IN ('pending','confirmed_pending_accounting')) FROM strategy_positions WHERE user_id=$1 AND token_address=$2 AND curve_address=$3`, ev.UserID, ev.TokenAddress, ev.CurveAddress).Scan(&amountRaw, &p.AverageCost, &p.FirstPrice, &p.LastPrice, &p.BuyCount, &p.SellCount, &p.ProfitLevel, &first, &next, &cool, &last, &p.PendingSell)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no rows") {
			return position{}, nil
		}
		return p, err
	}
	p.AmountRaw = integerText(amountRaw)
	p.Amount = rawFloat(p.AmountRaw)
	p.FirstBought, p.NextScheduled, p.CooldownUntil, p.LastBuy = first, next, cool, last
	return p, nil
}
func (e *Engine) savePosition(ctx context.Context, ev Event, p position) error {
	amountRaw := p.AmountRaw
	if amountRaw == "" {
		amountRaw = integerRaw(p.Amount)
	}
	_, err := e.Store.DB.Exec(ctx, `INSERT INTO strategy_positions(user_id,token_address,curve_address,token_amount_raw,average_cost_usd,first_buy_price,last_buy_price,buy_count,sell_count,profit_sell_level,first_bought_at,next_scheduled_sell_at,external_cooldown_until,last_buy_at,last_event_key,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,now()) ON CONFLICT(user_id,token_address,curve_address) DO UPDATE SET token_amount_raw=EXCLUDED.token_amount_raw,average_cost_usd=EXCLUDED.average_cost_usd,first_buy_price=EXCLUDED.first_buy_price,last_buy_price=EXCLUDED.last_buy_price,buy_count=EXCLUDED.buy_count,sell_count=EXCLUDED.sell_count,profit_sell_level=EXCLUDED.profit_sell_level,first_bought_at=EXCLUDED.first_bought_at,next_scheduled_sell_at=EXCLUDED.next_scheduled_sell_at,external_cooldown_until=EXCLUDED.external_cooldown_until,last_buy_at=EXCLUDED.last_buy_at,last_event_key=EXCLUDED.last_event_key,updated_at=now()`, ev.UserID, ev.TokenAddress, ev.CurveAddress, amountRaw, p.AverageCost, p.FirstPrice, p.LastPrice, p.BuyCount, p.SellCount, p.ProfitLevel, p.FirstBought, p.NextScheduled, p.CooldownUntil, p.LastBuy, ev.Key)
	return err
}

func (e *Engine) applyOwnFill(ctx context.Context, ev Event, p position, cfg Config) error {
	if ev.Price <= 0 {
		return nil
	}
	amountRawText := integerText(ev.TokenAmountText)
	amount := ev.TokenAmountRaw
	if amountRawText != "0" {
		amount = rawFloat(amountRawText)
	}
	if amount <= 0 {
		// quoteAmountRaw/tokenAmountRaw are raw pool units.  Prefer the raw
		// execution price when available; dividing raw quote units by a USD
		// price mixes units and badly corrupts the position on non-18-decimal
		// tokens or quote currencies.
		if ev.PriceRaw > 0 {
			amount = ev.QuoteAmountRaw / ev.PriceRaw
		} else if ev.QuoteUSD > 0 && ev.Price > 0 {
			amount = ev.QuoteUSD / ev.Price * math.Pow10(ev.TokenDecimals)
		}
	}
	if amount <= 0 {
		return nil
	}
	actionReason := "confirmed_fill"
	txHash := strings.Split(ev.Key, ":")[0]
	var pendingActionReason, pendingActionSide string
	var pendingActionAmount float64
	if txHash != "" {
		if err := e.Store.DB.QueryRow(ctx, `SELECT side,reason,amount FROM strategy_pending_actions WHERE transaction_hash=$1 AND user_id=$2 AND token_address=$3 AND curve_address=$4 AND status='pending'`, txHash, ev.UserID, ev.TokenAddress, ev.CurveAddress).Scan(&pendingActionSide, &pendingActionReason, &pendingActionAmount); err == nil {
			actionReason = pendingActionReason
			_, _ = e.Store.DB.Exec(ctx, `UPDATE strategy_pending_actions SET status='confirmed',updated_at=now() WHERE transaction_hash=$1`, txHash)
		}
	}
	if strings.ToLower(ev.Side) == "buy" {
		winner := ev.Key
		if i := strings.IndexByte(winner, ':'); i > 0 {
			winner = winner[:i]
		}
		_, _ = e.Store.DB.Exec(ctx, `UPDATE strategy_pending_buys SET status='confirmed',winner_transaction_hash=$4,updated_at=now() WHERE user_id=$1 AND token_address=$2 AND curve_address=$3 AND status IN ('pending','confirmed_pending_accounting')`, ev.UserID, ev.TokenAddress, ev.CurveAddress, winner)
		_, _ = e.Store.DB.Exec(ctx, `UPDATE strategy_buy_attempts SET status=CASE WHEN transaction_hash=$4 THEN 'confirmed' ELSE 'replaced' END,updated_at=now() WHERE user_id=$1 AND token_address=$2 AND curve_address=$3 AND status='pending'`, ev.UserID, ev.TokenAddress, ev.CurveAddress, winner)
		old := p.Amount
		oldRaw := new(big.Int)
		if _, ok := oldRaw.SetString(integerText(p.AmountRaw), 10); !ok {
			oldRaw.SetInt64(0)
		}
		fillRaw := new(big.Int)
		if _, ok := fillRaw.SetString(amountRawText, 10); !ok || fillRaw.Sign() <= 0 {
			fillRaw.SetString(integerRaw(amount), 10)
		}
		newRaw := new(big.Int).Add(oldRaw, fillRaw)
		p.AmountRaw = newRaw.String()
		p.Amount = rawFloat(p.AmountRaw)
		if old+amount > 0 {
			p.AverageCost = (old*p.AverageCost + amount*ev.Price) / (old + amount)
		}
		now := time.Now()
		if p.FirstPrice <= 0 {
			p.FirstPrice = ev.Price
			p.FirstBought = &now
		}
		p.LastPrice = ev.Price
		p.LastBuy = &now
		p.BuyCount++
		// Persist the confirmed fill separately from the strategy intent. The
		// intent may have been recorded at broadcast time, while this row is
		// written only after the own transaction event has supplied a real fill.
		tokenRawText := fillRaw.String()
		if tokenRawText == "" || tokenRawText == "0" {
			tokenRawText = integerRaw(amount)
		}
		quoteRawText := absIntegerText(ev.QuoteAmountText)
		if quoteRawText == "" {
			quoteRawText = integerRaw(ev.QuoteAmountRaw)
		}
		_, _ = e.Store.DB.Exec(ctx, `INSERT INTO monitor_records(user_id,token_address,curve_address,type,token_amount_raw,quote_amount_raw,remain_token_amount_raw,remain_quote_amount_raw,reason,source_event_key,transaction_hash,created_at) VALUES($1,$2,$3,'buy',$4,$5,$4,$5,'confirmed_fill',$6,$7,now()) ON CONFLICT(source_event_key) DO NOTHING`, ev.UserID, ev.TokenAddress, ev.CurveAddress, tokenRawText, quoteRawText, ev.Key, strings.Split(ev.Key, ":")[0])
		if cfg.SellPolicy.ResetSellCountOnBuy {
			p.SellCount = 0
		}
		if cfg.ProfitSell.ResetOnBuy {
			p.ProfitLevel = 0
		}
		if cfg.ScheduledSell.Enabled && (cfg.ScheduledSell.ResetOnBuy || p.NextScheduled == nil) {
			n := time.Now().Add(time.Duration(cfg.ScheduledSell.IntervalSecond) * time.Second)
			p.NextScheduled = &n
		}
	} else if strings.ToLower(ev.Side) == "sell" {
		oldRaw := new(big.Int)
		if _, ok := oldRaw.SetString(integerText(p.AmountRaw), 10); !ok {
			oldRaw.SetString(integerRaw(p.Amount), 10)
		}
		sellRaw := new(big.Int)
		if _, ok := sellRaw.SetString(amountRawText, 10); !ok || sellRaw.Sign() <= 0 {
			sellRaw.SetString(integerRaw(amount), 10)
		}
		if sellRaw.Cmp(oldRaw) > 0 {
			sellRaw.Set(oldRaw)
		}
		amount = rawFloat(sellRaw.String())
		remainingRaw := new(big.Int).Sub(oldRaw, sellRaw)
		if remainingRaw.Sign() <= 0 {
			p = position{}
		} else {
			p.AmountRaw = remainingRaw.String()
			p.Amount = rawFloat(p.AmountRaw)
		}
		if p.Amount > 0 {
			p.SellCount++
			if actionReason == "profit_sell" {
				p.ProfitLevel++
			}
			if actionReason == "scheduled_sell" && cfg.ScheduledSell.Enabled {
				n := time.Now().Add(time.Duration(cfg.ScheduledSell.IntervalSecond) * time.Second)
				p.NextScheduled = &n
			}
			if cfg.SellPolicy.FullSellAfterSellCount > 0 && p.SellCount > cfg.SellPolicy.FullSellAfterSellCount {
				p.Amount = 0
			}
		}
		quoteRawText := absIntegerText(ev.QuoteAmountText)
		if quoteRawText == "" {
			quoteRawText = integerRaw(ev.QuoteAmountRaw)
		}
		remainRawText := p.AmountRaw
		if remainRawText == "" {
			remainRawText = integerRaw(p.Amount)
		}
		_, _ = e.Store.DB.Exec(ctx, `INSERT INTO monitor_records(user_id,token_address,curve_address,type,token_amount_raw,quote_amount_raw,remain_token_amount_raw,remain_quote_amount_raw,reason,source_event_key,transaction_hash,created_at) VALUES($1,$2,$3,'sell',$4,$5,$6,$5,$7,$8,$9,now()) ON CONFLICT(source_event_key) DO NOTHING`, ev.UserID, ev.TokenAddress, ev.CurveAddress, sellRaw.String(), quoteRawText, remainRawText, actionReason, ev.Key, txHash)
	}
	e.devInfo("交易成交回执已确认",
		zap.Int64("userID", ev.UserID),
		zap.String("side", strings.ToLower(ev.Side)),
		zap.String("reason", actionReason),
		zap.String("transactionHash", txHash),
		zap.String("token", ev.TokenAddress),
		zap.String("curve", ev.CurveAddress),
	)
	return e.savePosition(ctx, ev, p)
}

func evaluate(c Config, r TokenRule, ev Event, p position) (string, float64, string) {
	price := ev.Price
	if price <= 0 {
		price = p.LastPrice
	}
	if price <= 0 {
		return "", 0, ""
	}
	if strings.ToLower(ev.Side) == "sell" {
		priceDrop := -ev.PriceChangeRatio
		impactDrop := -ev.PriceImpactRatio
		if ev.PriceImpactRatio == 0 {
			impactDrop = -ev.Impact
		}
		if impactDrop < r.MinSellRatio && priceDrop < r.MinSellRatio {
			return "", 0, ""
		}
		if p.LastBuy != nil && c.DiffBuySecond > 0 && time.Since(*p.LastBuy) < time.Duration(c.DiffBuySecond)*time.Second {
			return "", 0, ""
		}
		amt := r.BuyUSD
		if r.BuyRatio > 0 {
			amt = ev.QuoteUSD * r.BuyRatio
		}
		if amt <= 0 {
			return "", 0, ""
		}
		if p.FirstPrice > 0 && p.BuyCount > 0 && price >= p.FirstPrice {
			return "", 0, ""
		}
		if p.Amount > 0 && p.BuyCount > 0 && p.FirstPrice > 0 && price < p.FirstPrice {
			if c.MaxLossBuyTimes <= 0 || p.BuyCount >= c.MaxLossBuyTimes {
				return "", 0, ""
			}
		}
		if p.LastPrice > 0 && c.MinLossBuyRatio > 0 && price > p.LastPrice*(1-c.MinLossBuyRatio) {
			return "", 0, ""
		}
		return "buy", amt, "external_sell_signal"
	}
	if strings.ToLower(ev.Side) != "buy" || p.Amount <= 0 {
		return "", 0, ""
	}
	force := c.SellPolicy.FullSellAfterSellCount > 0 && p.SellCount >= c.SellPolicy.FullSellAfterSellCount
	if p.CooldownUntil != nil && time.Now().Before(*p.CooldownUntil) {
		// An already reached public full-sell threshold has priority over the
		// external-buy cooldown.  Other ladder/protection decisions are skipped
		// while the cooldown is active.
		if force && strings.ToLower(ev.Side) == "buy" {
			return "sell", p.Amount, "sell_count_force_full"
		}
		return "", 0, ""
	}
	gain := 0.0
	if p.AverageCost > 0 {
		gain = price/p.AverageCost - 1
	}
	impact := ev.PriceImpactRatio
	if impact == 0 {
		impact = ev.Impact
	}
	if c.ExternalBuySell.Enabled && (c.ExternalBuySell.MinBuyUSD <= 0 || ev.QuoteUSD >= c.ExternalBuySell.MinBuyUSD) && (c.ExternalBuySell.BuyImpactRatio <= 0 || impact >= c.ExternalBuySell.BuyImpactRatio) && (!c.ExternalBuySell.NeedProfit || gain >= c.ExternalBuySell.ProfitRatio) {
		if force {
			return "sell", p.Amount, "external_buy_signal"
		}
		if c.ExternalBuySell.BuyAmountRatio > 0 {
			a := 0.0
			if ev.PriceRaw > 0 && ev.QuoteAmountRaw > 0 {
				a = ev.QuoteAmountRaw * c.ExternalBuySell.BuyAmountRatio / ev.PriceRaw
			} else {
				a = ev.QuoteUSD * c.ExternalBuySell.BuyAmountRatio / price * math.Pow10(ev.TokenDecimals)
			}
			if a > p.Amount {
				a = p.Amount
			}
			return "sell", a, "external_buy_signal"
		}
		return "sell", p.Amount * c.ExternalBuySell.SellRatio, "external_buy_signal"
	}
	if c.ProfitSell.Enabled && p.ProfitLevel < len(c.ProfitSell.Levels) {
		l := c.ProfitSell.Levels[p.ProfitLevel]
		if gain >= l.ProfitRatio {
			if force {
				return "sell", p.Amount, "profit_sell"
			}
			return "sell", p.Amount * l.SellRatio, "profit_sell"
		}
	}
	if c.LossSell.Enabled && c.LossSell.TriggerRatio > 0 && gain <= -c.LossSell.TriggerRatio {
		if c.LossSell.SellAll || force {
			return "sell", p.Amount, "loss_protection"
		}
		return "", 0, ""
	}
	if force {
		return "sell", p.Amount, "sell_count_force_full"
	}
	return "", 0, ""
}

func (e *Engine) savePendingAction(ctx context.Context, hash string, ev Event, side string, amount float64, reason string) error {
	hash = strings.Split(hash, ":")[0]
	var alreadyFilled bool
	_ = e.Store.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM monitor_records WHERE transaction_hash=$1 AND user_id=$2 AND token_address=$3 AND curve_address=$4)`, hash, ev.UserID, ev.TokenAddress, ev.CurveAddress).Scan(&alreadyFilled)
	status := "pending"
	if alreadyFilled {
		status = "confirmed"
		if side == "sell" {
			if p, loadErr := e.loadPosition(ctx, ev); loadErr == nil {
				if reason == "profit_sell" {
					p.ProfitLevel++
				}
				interval := 0
				if u, userErr := e.Store.User(ctx, ev.UserID); userErr == nil {
					interval = Parse(u.Config).ScheduledSell.IntervalSecond
				}
				if reason == "scheduled_sell" && p.Amount > 0 && interval > 0 {
					n := time.Now().Add(time.Duration(interval) * time.Second)
					p.NextScheduled = &n
				}
				_ = e.savePosition(ctx, ev, p)
			}
		}
	}
	_, err := e.Store.DB.Exec(ctx, `INSERT INTO strategy_pending_actions(transaction_hash,user_id,token_address,curve_address,side,reason,amount,status,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,now()) ON CONFLICT(transaction_hash) DO UPDATE SET amount=EXCLUDED.amount,reason=EXCLUDED.reason,status=EXCLUDED.status,updated_at=now()`, hash, ev.UserID, ev.TokenAddress, ev.CurveAddress, side, reason, amount, status)
	return err
}

func (e *Engine) Tick(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = e.refreshMonitorCache(ctx, false)
			e.checkScheduled(ctx)
			e.pollCurves(ctx)
			if time.Now().Unix()%5 == 0 {
				e.reconcilePending(ctx)
			}
		}
	}
}

// reconcilePending makes accepted broadcasts survive a service restart. A
// receipt is never treated as a fill by itself: its decoded logs are replayed
// through the normal monitor path, where own-address matching, idempotency and
// position accounting remain centralized.
func (e *Engine) reconcilePending(ctx context.Context) {
	if e.RPC == nil || e.Store == nil {
		return
	}
	rows, err := e.Store.DB.Query(ctx, `SELECT transaction_hash FROM strategy_pending_buys WHERE status IN ('pending','confirmed_pending_accounting') AND transaction_hash IS NOT NULL UNION SELECT transaction_hash FROM strategy_buy_attempts WHERE status='pending' UNION SELECT transaction_hash FROM strategy_pending_actions WHERE status='pending'`)
	if err != nil {
		return
	}
	defer rows.Close()
	hashes := map[string]struct{}{}
	for rows.Next() {
		var hash string
		if rows.Scan(&hash) == nil && hash != "" {
			hashes[hash] = struct{}{}
		}
	}
	for hash := range hashes {
		receipt, re := e.RPC.Receipt(ctx, hash)
		if re != nil || receipt == nil {
			continue
		}
		if revertedReceipt(receipt) {
			_, _ = e.Store.DB.Exec(ctx, `UPDATE strategy_pending_buys SET status='reverted',updated_at=now() WHERE transaction_hash=$1 AND status='pending'`, hash)
			_, _ = e.Store.DB.Exec(ctx, `UPDATE strategy_buy_attempts SET status='reverted',updated_at=now() WHERE transaction_hash=$1 AND status='pending'`, hash)
			_, _ = e.Store.DB.Exec(ctx, `UPDATE strategy_pending_actions SET status='reverted',updated_at=now() WHERE transaction_hash=$1 AND status='pending'`, hash)
			continue
		}
		_, _ = e.Store.DB.Exec(ctx, `UPDATE strategy_pending_buys SET status='confirmed_pending_accounting',updated_at=now() WHERE transaction_hash=$1 AND status='pending'`, hash)
		m, ok := receipt.(map[string]any)
		if !ok {
			continue
		}
		logs, ok := m["logs"].([]any)
		if !ok {
			continue
		}
		for _, raw := range logs {
			if log, ok := raw.(map[string]any); ok {
				decoded := chain.DecodeChainEvent(log)
				if ev, good := decodeEvent(decoded); good {
					_ = e.processForMonitors(ctx, ev)
				}
			}
		}
	}
}

func revertedReceipt(receipt any) bool {
	m, ok := receipt.(map[string]any)
	if !ok {
		return false
	}
	switch v := m["status"].(type) {
	case string:
		return v == "0x0" || v == "0x00" || v == "0"
	case float64:
		return v == 0
	}
	return false
}

func (e *Engine) pollCurves(ctx context.Context) {
	if e.RPC == nil || e.Store == nil {
		return
	}
	e.pollMu.Lock()
	defer e.pollMu.Unlock()
	headHex, err := e.RPC.LatestBlock(ctx)
	if err != nil {
		return
	}
	head := new(big.Int)
	if _, ok := head.SetString(strings.TrimPrefix(headHex, "0x"), 16); !ok {
		return
	}
	if err := e.refreshMonitorCache(ctx, false); err != nil {
		return
	}
	e.monitorMu.RLock()
	tokenSnapshot := make(map[int64][]store.Token, len(e.monitorTokens))
	for uid, tokens := range e.monitorTokens {
		tokenSnapshot[uid] = append([]store.Token(nil), tokens...)
	}
	e.monitorMu.RUnlock()
	curves := map[string]struct{}{}
	for _, tokens := range tokenSnapshot {
		for _, token := range tokens {
			if commonCurve(token.CurveAddress) {
				curves[strings.ToLower(token.CurveAddress)] = struct{}{}
			}
		}
	}
	// Curves are independent positions.  Bound concurrent RPC calls so a
	// large monitor list does not flood the node, while allowing slow curves
	// to be polled in parallel with the rest.
	var wg sync.WaitGroup
	sem := make(chan struct{}, 8)
	for curve := range curves {
		curve := curve
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			from := new(big.Int).Set(head)
			if v, ok := e.curveBlocks.Load(curve); ok {
				if n, ok2 := v.(*big.Int); ok2 {
					from = new(big.Int).Add(n, big.NewInt(1))
				}
			} else if from.Sign() > 0 {
				from.Sub(from, big.NewInt(2))
			}
			if from.Cmp(head) > 0 {
				return
			}
			allOK := true
			decodedLogs := make([]map[string]any, 0)
			// eth_getLogs accepts an OR-list for the first topic.  Fetching buy and
			// sell logs in one request removes one network round trip per curve on
			// every polling tick while preserving the later block/log ordering.
			logs, le := e.RPC.Logs(ctx, map[string]any{
				"address":   curve,
				"topics":    []any{[]string{chain.CurveBuyTopic, chain.CurveSellTopic}},
				"fromBlock": "0x" + fmt.Sprintf("%x", from),
				"toBlock":   "0x" + fmt.Sprintf("%x", head),
			})
			if le != nil {
				allOK = false
			} else if arr, ok := logs.([]any); ok {
				for _, raw := range arr {
					if m, ok := raw.(map[string]any); ok {
						decodedLogs = append(decodedLogs, chain.DecodeChainEvent(m))
					}
				}
			}
			sort.SliceStable(decodedLogs, func(i, j int) bool {
				bi := hexNumber(firstString(decodedLogs[i], "blockNumber", "block"))
				bj := hexNumber(firstString(decodedLogs[j], "blockNumber", "block"))
				li := hexNumber(firstString(decodedLogs[i], "logIndex"))
				lj := hexNumber(firstString(decodedLogs[j], "logIndex"))
				bc := bi.Cmp(bj)
				return bc < 0 || (bc == 0 && li.Cmp(lj) < 0)
			})
			for _, decoded := range decodedLogs {
				if ev, good := decodeEvent(decoded); good {
					_ = e.processForMonitors(ctx, ev)
				}
			}
			if allOK {
				e.curveBlocks.Store(curve, new(big.Int).Set(head))
			}
		}()
	}
	wg.Wait()
}

func commonCurve(s string) bool { return s != "" && !strings.EqualFold(s, chain.NativeAddress) }
func hexNumber(s string) *big.Int {
	n := new(big.Int)
	s = strings.TrimPrefix(strings.TrimPrefix(s, "0x"), "0X")
	if _, ok := n.SetString(s, 16); !ok {
		n.SetString("0", 10)
	}
	return n
}
func (e *Engine) checkScheduled(ctx context.Context) {
	rows, err := e.Store.DB.Query(ctx, `SELECT user_id,token_address,curve_address,token_amount_raw::text,average_cost_usd::double precision,first_buy_price::double precision,last_buy_price::double precision,buy_count,sell_count,profit_sell_level,first_bought_at,next_scheduled_sell_at,external_cooldown_until,last_buy_at FROM strategy_positions WHERE next_scheduled_sell_at IS NOT NULL AND next_scheduled_sell_at<=now() AND token_amount_raw>0`)
	if err != nil {
		return
	}
	defer rows.Close()
	for rows.Next() {
		var ev Event
		var p position
		if rows.Scan(&ev.UserID, &ev.TokenAddress, &ev.CurveAddress, &p.AmountRaw, &p.AverageCost, &p.FirstPrice, &p.LastPrice, &p.BuyCount, &p.SellCount, &p.ProfitLevel, &p.FirstBought, &p.NextScheduled, &p.CooldownUntil, &p.LastBuy) != nil {
			continue
		}
		p.AmountRaw = integerText(p.AmountRaw)
		p.Amount = rawFloat(p.AmountRaw)
		u, err := e.cachedUser(ctx, ev.UserID)
		if err != nil {
			continue
		}
		c := Parse(u.Config)
		if !c.Enabled || !c.ScheduledSell.Enabled {
			continue
		}
		price := p.LastPrice
		if price <= 0 {
			continue
		}
		token, tokenErr := e.Store.Token(ctx, ev.UserID, ev.TokenAddress, ev.CurveAddress)
		if tokenErr != nil {
			continue
		}
		tokenDecimals := 0
		if token.Decimals != nil {
			tokenDecimals = *token.Decimals
		}
		forceFull := c.SellPolicy.FullSellAfterSellCount > 0 && p.SellCount >= c.SellPolicy.FullSellAfterSellCount
		amount := p.Amount
		if !forceFull && c.ScheduledSell.SellRatio > 0 {
			amount = p.Amount * c.ScheduledSell.SellRatio
		}
		if c.ScheduledSell.BaseUSD > 0 && price > 0 {
			cap := c.ScheduledSell.BaseUSD / price * math.Pow10(tokenDecimals)
			if amount > cap {
				amount = cap
			}
		}
		if amount <= 0 {
			continue
		}
		e.devInfo("定时卖出触发",
			zap.Int64("userID", ev.UserID),
			zap.String("configName", c.Name),
			zap.String("action", "sell"),
			zap.String("reason", "scheduled_sell"),
			zap.Float64("amount", amount),
			zap.String("token", ev.TokenAddress),
			zap.String("curve", ev.CurveAddress),
		)
		var pendingScheduled bool
		if err := e.Store.DB.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM strategy_pending_actions WHERE user_id=$1 AND token_address=$2 AND curve_address=$3 AND side='sell' AND reason='scheduled_sell' AND status='pending')`, ev.UserID, ev.TokenAddress, ev.CurveAddress).Scan(&pendingScheduled); err == nil && pendingScheduled {
			continue
		}
		ev.Key = fmt.Sprintf("scheduled:%d:%s:%d", ev.UserID, ev.TokenAddress, time.Now().Unix())
		ev.Side = "sell"
		ev.Price = price
		ev.PriceRaw = rawTokenPriceFromUSD(price, token)
		ev.TokenDecimals = tokenDecimals
		if token.QuoteDecimals != nil {
			ev.QuoteDecimals = *token.QuoteDecimals
		}
		ev.QuoteUSD = amount / math.Pow10(tokenDecimals) * price
		var result map[string]any
		if e.ExecuteAction != nil {
			var ee error
			result, ee = e.ExecuteAction(ctx, ev, "sell", amount)
			if ee != nil {
				continue
			}
		} else if e.Trading != nil {
			var ee error
			result, ee = e.executeLive(ctx, ev, "sell", amount, c)
			if ee != nil {
				continue
			}
		}
		if hash := firstString(result, "transactionHash", "hash"); hash != "" {
			if e.savePendingAction(ctx, hash, ev, "sell", amount, "scheduled_sell") != nil {
				continue
			}
		}
		_ = e.savePosition(ctx, ev, p)
	}
}
