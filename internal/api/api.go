package api

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/websocket/v2"
	"github.com/google/uuid"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
	"math/big"
	"os"
	"path/filepath"
	"robinhood-go/internal/chain"
	"robinhood-go/internal/config"
	secret "robinhood-go/internal/crypto"
	"robinhood-go/internal/httpx"
	"robinhood-go/internal/store"
	"robinhood-go/internal/strategy"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type API struct {
	Cfg                    config.Config
	Store                  *store.Store
	RPC                    *chain.RPC
	Trading                *chain.Trading
	Redis                  *redis.Client
	Log                    *zap.Logger
	invalidateMonitorCache func()
	clients                map[*websocket.Conn]struct{}
	clientsMu              sync.RWMutex
	aveAuthMu              sync.Mutex
	eventsMu               sync.RWMutex
	deployments            []map[string]any
	swaps                  []map[string]any
}

func New(c config.Config, s *store.Store, r *chain.RPC, t *chain.Trading, rd *redis.Client, l *zap.Logger) *API {
	return &API{Cfg: c, Store: s, RPC: r, Trading: t, Redis: rd, Log: l, clients: map[*websocket.Conn]struct{}{}}
}

func (a *API) SetMonitorCacheInvalidator(fn func()) { a.invalidateMonitorCache = fn }

func (a *API) invalidateCache() {
	if a.invalidateMonitorCache != nil {
		a.invalidateMonitorCache()
	}
}
func (a *API) Register(app *fiber.App) {
	app.Get("/ping", func(c *fiber.Ctx) error { return httpx.OK(c, map[string]any{"status": "ok"}) })
	app.Get("/health", func(c *fiber.Ctx) error {
		out := map[string]any{"status": "ok", "database": a.Store != nil}
		if a.RPC != nil {
			out["wssDroppedEvents"] = a.RPC.DroppedEvents()
			out["wssDroppedSubscriberEvents"] = a.RPC.DroppedSubscriberEvents()
		}
		return httpx.OK(c, out)
	})
	g := app.Group("/api/v1")
	app.Get("/api/docs", func(c *fiber.Ctx) error {
		c.Set("content-type", "text/html; charset=utf-8")
		return c.SendString(`<html><head><title>RobinhoodGo API</title></head><body><h1>RobinhoodGo API</h1><p>See <a href="/docs/FUNCTIONAL_SPEC.md">functional specification</a>.</p><p>All endpoints are under <code>/api/v1</code>.</p></body></html>`)
	})
	g.Get("/chain/network", a.network)
	g.Get("/chain/block/latest", a.latestBlock)
	g.Get("/chain/balance/:address", a.balance)
	g.Get("/chain/transactions/:hash", a.transaction)
	g.Get("/chain/pons-v2/listener", a.listener)
	g.Get("/chain/pons-v2/deployments", a.deploymentsList)
	g.Get("/chain/pons-v2/swaps", a.swapsList)
	g.Get("/chain/pons-v2/deployments/:hash", a.deploymentGet)
	b := g.Group("/chain/bottom-fishing")
	b.Post("/ave/getConfig", a.aveGetConfig)
	b.Post("/ave/updateConfig", a.aveUpdateConfig)
	b.Post("/monitorUser/getUserList", a.userList)
	b.Post("/monitorUser/getUser", a.userGet)
	b.Post("/monitorUser/updateUser", a.userUpdate)
	b.Post("/monitorUser/deleteUser", a.userDelete)
	b.Post("/monitorUser/getPrivateKey", a.userPrivate)
	b.Post("/monitorUser/getUserHolders", a.positions)
	b.Post("/monitorUser/getAvePositions", a.positions)
	b.Post("/monitorToken/getList", a.tokenList)
	b.Post("/monitorToken/getDisabledList", a.tokenDisabled)
	b.Post("/monitorToken/addToken", a.tokenAdd)
	b.Post("/monitorToken/deleteToken", a.tokenDelete)
	b.Post("/monitorToken/enableToken", a.tokenEnable)
	b.Post("/monitorToken/disableToken", a.tokenDisable)
	b.Post("/monitorToken/syncTokens", a.syncTokens)
	b.Post("/monitorToken/getAveList", a.aveList)
	b.Post("/monitorToken/startAveToken", a.startAve)
	b.Post("/monitorRoute/getList", a.routeList)
	b.Post("/monitorRoute/save", a.routeSave)
	b.Post("/monitorRoute/delete", a.routeDelete)
	p := g.Group("/chain/pons-v2")
	p.Post("/quote", a.ponsQuote)
	p.Post("/buy", a.ponsBuy)
	p.Post("/sell", a.ponsSell)
	u := g.Group("/chain/uniswap-v4")
	u.Post("/quote", a.quote)
	u.Post("/route/quote", a.quote)
	u.Post("/buy", a.swap)
	u.Post("/sell", a.swap)
	u.Post("/route/swap", a.swap)
	files := g.Group("/files")
	files.Get("/", a.fileList)
	files.Get("/:id", a.fileGet)
	files.Delete("/:id", a.fileDelete)
	files.Post("/cleanup-orphaned", a.fileCleanup)
	files.Post("/upload", a.fileUpload)
	files.Post("/upload-multiple", a.fileUploadMultiple)
	g.Get("/logger", a.logs)
	app.Get("/ws", websocket.New(a.ws))
}
func body(c *fiber.Ctx) map[string]any {
	var v map[string]any
	_ = json.Unmarshal(c.Body(), &v)
	if v == nil {
		v = map[string]any{}
	}
	return v
}
func id(v any) int64 {
	switch x := v.(type) {
	case float64:
		return int64(x)
	case json.Number:
		n, _ := strconv.ParseInt(string(x), 10, 64)
		return n
	case string:
		n, _ := strconv.ParseInt(x, 10, 64)
		return n
	}
	return 0
}
func str(v any) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}
func bigIntToInt64(v *big.Int) int64 {
	if v == nil {
		return 0
	}
	return v.Int64()
}
func low(v string) string                   { return strings.ToLower(strings.TrimSpace(v)) }
func (a *API) ok(c *fiber.Ctx, v any) error { return httpx.OK(c, v) }
func (a *API) fail(c *fiber.Ctx, e error) error {
	if e == nil {
		return nil
	}
	return httpx.Error(c, 400, e.Error())
}
func (a *API) network(c *fiber.Ctx) error {
	network := "mainnet"
	if a.Cfg.ChainID == 46630 {
		network = "testnet"
	}
	name := a.Cfg.ChainName
	if name == "" || strings.EqualFold(name, "robinhood") {
		name = "Robinhood Chain"
	}
	return a.ok(c, map[string]any{"name": name, "network": network, "chainId": a.Cfg.ChainID, "nativeCurrency": map[string]any{"name": "ETH", "symbol": "ETH", "decimals": 18}, "rpcConfigured": a.Cfg.RPCURL != ""})
}
func (a *API) latestBlock(c *fiber.Ctx) error {
	v, e := a.RPC.LatestBlock(c.Context())
	if e != nil {
		return a.fail(c, e)
	}
	dec := new(big.Int)
	if strings.HasPrefix(v, "0x") {
		dec.SetString(v[2:], 16)
	} else {
		dec.SetString(v, 10)
	}
	chainID := fmt.Sprintf("0x%x", a.Cfg.ChainID)
	if raw, ce := a.RPC.Call(c.Context(), "eth_chainId", []any{}); ce == nil {
		_ = json.Unmarshal(raw, &chainID)
	}
	chainDec := new(big.Int)
	if strings.HasPrefix(chainID, "0x") {
		chainDec.SetString(chainID[2:], 16)
	}
	return a.ok(c, map[string]any{"blockNumberHex": v, "blockNumber": dec.String(), "chainIdHex": chainID, "chainId": chainDec.String()})
}
func (a *API) balance(c *fiber.Ctx) error {
	if !common.IsHexAddress(c.Params("address")) {
		return httpx.Error(c, 400, "Invalid EVM address")
	}
	v, e := a.RPC.Balance(c.Context(), c.Params("address"))
	if e != nil {
		return a.fail(c, e)
	}
	wei := new(big.Int)
	if strings.HasPrefix(v, "0x") {
		wei.SetString(v[2:], 16)
	}
	network := "mainnet"
	if a.Cfg.ChainID == 46630 {
		network = "testnet"
	}
	return a.ok(c, map[string]any{"address": low(c.Params("address")), "balanceWei": v, "balance": new(big.Float).Quo(new(big.Float).SetInt(wei), new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))).Text('f', 18), "symbol": "ETH", "network": network})
}
func (a *API) transaction(c *fiber.Ctx) error {
	hash := c.Params("hash")
	if len(hash) != 66 || !strings.HasPrefix(hash, "0x") {
		return httpx.Error(c, 400, "Invalid transaction hash")
	}
	if _, decodeErr := hex.DecodeString(hash[2:]); decodeErr != nil {
		return httpx.Error(c, 400, "Invalid transaction hash")
	}
	v, e := a.RPC.Tx(c.Context(), hash)
	if e != nil {
		return a.fail(c, e)
	}
	result := map[string]any{"hash": strings.ToLower(hash), "network": a.Cfg.ChainName, "transaction": v, "receipt": nil}
	if m, ok := v.(map[string]any); ok {
		result["transaction"], result["receipt"] = m["transaction"], m["receipt"]
	}
	return a.ok(c, result)
}
func (a *API) listener(c *fiber.Ctx) error {
	n := 0
	if a.Cfg.DeploymentListener {
		n++
	}
	if a.Cfg.SwapListener {
		n++
	}
	return a.ok(c, map[string]any{"deploymentEnabled": a.Cfg.DeploymentListener, "swapEnabled": a.Cfg.SwapListener, "subscriptionCount": n})
}
func boolInt(v bool) int {
	if v {
		return 1
	}
	return 0
}
func (a *API) empty(c *fiber.Ctx) error { return a.ok(c, []any{}) }
func (a *API) deploymentsList(c *fiber.Ctx) error {
	a.eventsMu.RLock()
	defer a.eventsMu.RUnlock()
	out := append([]map[string]any(nil), a.deployments...)
	return a.ok(c, out)
}
func (a *API) swapsList(c *fiber.Ctx) error {
	a.eventsMu.RLock()
	defer a.eventsMu.RUnlock()
	limit, _ := strconv.Atoi(c.Query("limit", "100"))
	if limit < 1 {
		limit = 1
	}
	if limit > 1000 {
		limit = 1000
	}
	pool := strings.ToLower(c.Query("poolId"))
	out := make([]map[string]any, 0, limit)
	for i := len(a.swaps) - 1; i >= 0 && len(out) < limit; i-- {
		if pool != "" && strings.ToLower(str(a.swaps[i]["poolId"])) != pool {
			continue
		}
		out = append(out, a.swaps[i])
	}
	return a.ok(c, out)
}
func (a *API) deploymentGet(c *fiber.Ctx) error {
	h := strings.ToLower(c.Params("hash"))
	a.eventsMu.RLock()
	defer a.eventsMu.RUnlock()
	for _, d := range a.deployments {
		if strings.ToLower(str(d["transactionHash"])) == h {
			return a.ok(c, d)
		}
	}
	return httpx.Error(c, 404, "Pons V2 deployment event not found")
}
func (a *API) aveGetConfig(c *fiber.Ctx) error {
	x, err := a.aveAuth(c.Context())
	if err != nil {
		return a.fail(c, err)
	}
	return a.ok(c, map[string]any{"id": 1, "xAuth": x})
}
func (a *API) aveUpdateConfig(c *fiber.Ctx) error {
	x := strings.TrimSpace(str(body(c)["xAuth"]))
	if x == "" {
		return httpx.Error(c, 400, "xAuth is required")
	}
	a.aveAuthMu.Lock()
	defer a.aveAuthMu.Unlock()
	e := a.saveAveAuth(c.Context(), x)
	if e != nil {
		return a.fail(c, e)
	}
	return a.ok(c, map[string]any{"id": 1, "xAuth": x})
}
func (a *API) userList(c *fiber.Ctx) error {
	u, e := a.Store.Users(c.Context())
	if e != nil {
		return a.fail(c, e)
	}
	out := make([]map[string]any, 0, len(u))
	for _, user := range u {
		out = append(out, a.userView(user, false))
	}
	sort.SliceStable(out, func(i, j int) bool {
		return fmt.Sprint(out[i]["updatedAt"]) > fmt.Sprint(out[j]["updatedAt"])
	})
	return a.ok(c, out)
}
func (a *API) userGet(c *fiber.Ctx) error {
	d := body(c)
	u, e := a.Store.User(c.Context(), id(d["userId"]))
	if e != nil {
		return httpx.Error(c, 404, "user not found")
	}
	return a.ok(c, a.userView(u, true))
}
func (a *API) userUpdate(c *fiber.Ctx) error {
	d := body(c)
	uid := id(d["userId"])
	var current store.User
	var hasCurrent bool
	if uid > 0 {
		if existing, err := a.Store.User(c.Context(), uid); err == nil {
			current, hasCurrent = existing, true
		}
	}
	if uid == 0 {
		uid, _ = a.Store.NextUserID(c.Context())
	}
	cfg := map[string]any{}
	if hasCurrent {
		for k, v := range current.Config {
			cfg[k] = v
		}
	}
	if x, ok := d["config"].(map[string]any); ok {
		for k, v := range x {
			cfg[k] = v
		}
	}
	for _, k := range []string{"enabled", "minMcp", "slippage", "diffBuySecond", "autoSellSecond", "autoProfitRatio", "autoLossRatio", "maxLossBuyTimes", "minLossBuyRatio", "onceSellMinRatio", "replacePending", "tokenConfig", "sellPolicy", "scheduledSell", "externalBuySell", "profitSell", "lossSell", "listConfig"} {
		if v, ok := d[k]; ok {
			cfg[k] = v
		}
	}
	if _, ok := cfg["enabled"]; !ok {
		cfg["enabled"] = true
	}
	enabled := true
	if hasCurrent {
		enabled = current.Enabled
	}
	if v, ok := d["enabled"].(bool); ok {
		enabled = v
	}
	wallet := low(str(d["walletAddress"]))
	encrypted := str(d["privateKeyEncrypted"])
	if wallet == "" {
		if hasCurrent && current.WalletAddress != "" {
			wallet = low(current.WalletAddress)
		} else {
			if a.Cfg.EncryptionKey == "" {
				return httpx.Error(c, 503, "monitor wallet encryption key is not configured")
			}
			key, _ := crypto.GenerateKey()
			wallet = low(crypto.PubkeyToAddress(key.PublicKey).Hex())
			if encrypted == "" {
				encrypted, _ = secret.Encrypt(hex.EncodeToString(crypto.FromECDSA(key)), a.Cfg.EncryptionKey)
			}
		}
	}
	name := str(d["name"])
	if name == "" {
		name = str(cfg["name"])
	}
	if name == "" && hasCurrent {
		name = str(current.Config["name"])
	}
	u, e := a.Store.UpsertUser(c.Context(), uid, name, wallet, encrypted, cfg, enabled)
	if e != nil {
		return a.fail(c, e)
	}
	a.invalidateCache()
	return a.ok(c, a.userView(u, false))
}

type apiSellPolicyView struct {
	FullSellAfterSellCount int  `json:"fullSellAfterSellCount"`
	ResetSellCountOnBuy    bool `json:"resetSellCountOnBuy"`
}

type apiScheduledSellView struct {
	Enabled        bool    `json:"enabled"`
	IntervalSecond int     `json:"intervalSecond"`
	SellRatio      float64 `json:"sellRatio"`
	BaseUSD        float64 `json:"baseUSD"`
	ResetOnBuy     bool    `json:"resetOnBuy"`
}

type apiExternalBuySellView struct {
	Enabled        bool    `json:"enabled"`
	MinBuyUSD      float64 `json:"minBuyUSD"`
	BuyImpactRatio float64 `json:"buyImpactRatio"`
	NeedProfit     bool    `json:"needProfit"`
	ProfitRatio    float64 `json:"profitRatio"`
	SellRatio      float64 `json:"sellRatio"`
	BuyAmountRatio float64 `json:"buyAmountRatio"`
	CooldownSecond int     `json:"cooldownSecond"`
}

type apiProfitLevelView struct {
	ProfitRatio float64 `json:"profitRatio"`
	SellRatio   float64 `json:"sellRatio"`
}

type apiProfitSellView struct {
	Enabled    bool                 `json:"enabled"`
	ResetOnBuy bool                 `json:"resetOnBuy"`
	Levels     []apiProfitLevelView `json:"levels"`
}

type apiLossSellView struct {
	Enabled      bool    `json:"enabled"`
	TriggerRatio float64 `json:"triggerRatio"`
	SellAll      bool    `json:"sellAll"`
}

type apiTokenRuleView struct {
	MaxMCP       float64 `json:"maxMcp"`
	BuyUSD       float64 `json:"buyUSD"`
	BuyRatio     float64 `json:"buyRatio"`
	MinSellRatio float64 `json:"minSellRatio"`
}

type apiListConfigView struct {
	MaxMCP    float64 `json:"maxMcp"`
	MinMCP    float64 `json:"minMcp"`
	Source    string  `json:"source"`
	Category  string  `json:"category"`
	CreateDay int     `json:"createDay"`
}

// apiConfigView is deliberately a struct: encoding a map cannot guarantee
// the field order required by the existing frontend response contract.
type apiConfigView struct {
	Name            string                 `json:"name"`
	MinMCP          float64                `json:"minMcp"`
	Slippage        float64                `json:"slippage"`
	DiffBuySecond   int                    `json:"diffBuySecond"`
	MaxLossBuyTimes int                    `json:"maxLossBuyTimes"`
	MinLossBuyRatio float64                `json:"minLossBuyRatio"`
	ReplacePending  bool                   `json:"replacePending"`
	SellPolicy      apiSellPolicyView      `json:"sellPolicy"`
	ScheduledSell   apiScheduledSellView   `json:"scheduledSell"`
	ExternalBuySell apiExternalBuySellView `json:"externalBuySell"`
	ProfitSell      apiProfitSellView      `json:"profitSell"`
	LossSell        apiLossSellView        `json:"lossSell"`
	TokenConfig     []apiTokenRuleView     `json:"tokenConfig"`
	ListConfig      apiListConfigView      `json:"listConfig"`
}

// userView mirrors the Node service's public monitor-user DTO. Database
// columns such as the internal id and encrypted key are deliberately omitted;
// configuration is normalized so partial legacy rows return the same shape as
// newly-created users.
func (a *API) userView(u store.User, withBalance bool) map[string]any {
	cfg := strategy.Parse(u.Config)
	name := cfg.Name
	if name == "" || name == "strategy" {
		name = fmt.Sprintf("User%d", u.UserID)
	}
	rules := make([]apiTokenRuleView, 0, len(cfg.TokenConfig))
	for _, rule := range cfg.TokenConfig {
		rules = append(rules, apiTokenRuleView{MaxMCP: rule.MaxMCP, BuyUSD: rule.BuyUSD, BuyRatio: rule.BuyRatio, MinSellRatio: rule.MinSellRatio})
	}
	levels := make([]apiProfitLevelView, 0, len(cfg.ProfitSell.Levels))
	for _, level := range cfg.ProfitSell.Levels {
		levels = append(levels, apiProfitLevelView{ProfitRatio: level.ProfitRatio, SellRatio: level.SellRatio})
	}
	public := apiConfigView{
		Name: name, MinMCP: cfg.MinMCP, Slippage: cfg.Slippage,
		DiffBuySecond: cfg.DiffBuySecond, MaxLossBuyTimes: cfg.MaxLossBuyTimes,
		MinLossBuyRatio: cfg.MinLossBuyRatio, ReplacePending: cfg.ReplacePending,
		SellPolicy:      apiSellPolicyView{FullSellAfterSellCount: cfg.SellPolicy.FullSellAfterSellCount, ResetSellCountOnBuy: cfg.SellPolicy.ResetSellCountOnBuy},
		ScheduledSell:   apiScheduledSellView{Enabled: cfg.ScheduledSell.Enabled, IntervalSecond: cfg.ScheduledSell.IntervalSecond, SellRatio: cfg.ScheduledSell.SellRatio, BaseUSD: cfg.ScheduledSell.BaseUSD, ResetOnBuy: cfg.ScheduledSell.ResetOnBuy},
		ExternalBuySell: apiExternalBuySellView{Enabled: cfg.ExternalBuySell.Enabled, MinBuyUSD: cfg.ExternalBuySell.MinBuyUSD, BuyImpactRatio: cfg.ExternalBuySell.BuyImpactRatio, NeedProfit: cfg.ExternalBuySell.NeedProfit, ProfitRatio: cfg.ExternalBuySell.ProfitRatio, SellRatio: cfg.ExternalBuySell.SellRatio, BuyAmountRatio: cfg.ExternalBuySell.BuyAmountRatio, CooldownSecond: cfg.ExternalBuySell.CooldownSecond},
		ProfitSell:      apiProfitSellView{Enabled: cfg.ProfitSell.Enabled, ResetOnBuy: cfg.ProfitSell.ResetOnBuy, Levels: levels},
		LossSell:        apiLossSellView{Enabled: cfg.LossSell.Enabled, TriggerRatio: cfg.LossSell.TriggerRatio, SellAll: cfg.LossSell.SellAll},
		TokenConfig:     rules,
		ListConfig:      apiListConfigView{MaxMCP: cfg.ListConfig.MaxMCP, MinMCP: cfg.ListConfig.MinMCP, Source: cfg.ListConfig.Source, Category: cfg.ListConfig.Category, CreateDay: cfg.ListConfig.CreateDay},
	}
	out := map[string]any{"userId": u.UserID, "walletAddress": strings.ToLower(u.WalletAddress), "config": public, "enabled": u.Enabled, "createdAt": u.CreatedAt, "updatedAt": u.UpdatedAt}
	if withBalance && a.RPC != nil {
		if raw, err := a.RPC.Balance(context.Background(), u.WalletAddress); err == nil {
			wei := strings.TrimPrefix(strings.Trim(raw, `"`), "0x")
			n := new(big.Int)
			if _, ok := n.SetString(wei, 16); ok {
				out["balanceWei"] = "0x" + wei
				balance := new(big.Float).Quo(new(big.Float).SetInt(n), new(big.Float).SetFloat64(1e18)).Text('f', 18)
				balance = strings.TrimRight(strings.TrimRight(balance, "0"), ".")
				if balance == "" {
					balance = "0"
				}
				out["balance"] = balance
				out["balanceSymbol"] = "ETH"
			} else {
				out["balance"], out["balanceWei"], out["balanceSymbol"] = nil, nil, nil
			}
		} else {
			out["balance"], out["balanceWei"], out["balanceSymbol"] = nil, nil, nil
		}
	}
	return out
}
func (a *API) userDelete(c *fiber.Ctx) error {
	d := body(c)
	if str(d["password"]) != a.Cfg.ExportPassword {
		return httpx.Error(c, 400, "invalid password")
	}
	e := a.Store.DeleteUser(c.Context(), id(d["userId"]))
	if e != nil {
		return a.fail(c, e)
	}
	a.invalidateCache()
	return a.ok(c, map[string]any{"deleted": true})
}
func (a *API) userPrivate(c *fiber.Ctx) error {
	d := body(c)
	if str(d["password"]) != a.Cfg.ExportPassword {
		return httpx.Error(c, 400, "invalid password")
	}
	u, e := a.Store.User(c.Context(), id(d["userId"]))
	if e != nil || u.PrivateKeyEncrypted == nil {
		return httpx.Error(c, 404, "private key not found")
	}
	p, e := secret.Decrypt(*u.PrivateKeyEncrypted, a.Cfg.EncryptionKey)
	if e != nil {
		return a.fail(c, e)
	}
	return a.ok(c, map[string]any{"userId": u.UserID, "privateKey": p})
}
func (a *API) positions(c *fiber.Ctx) error {
	d := body(c)
	u, e := a.Store.User(c.Context(), id(d["userId"]))
	if e != nil {
		return httpx.Error(c, 404, "user not found")
	}
	items, err := a.aveWalletTokens(c.Context(), u.WalletAddress)
	if err != nil {
		return a.fail(c, err)
	}
	if strings.HasSuffix(c.Path(), "getUserHolders") {
		filtered := items[:0]
		for _, item := range items {
			value := item["balance_usd"]
			amount := 0.0
			switch x := value.(type) {
			case float64:
				amount = x
			case string:
				amount, _ = strconv.ParseFloat(x, 64)
			}
			if amount >= 0.1 || value == nil {
				filtered = append(filtered, item)
			}
		}
		items = filtered
	}
	return a.ok(c, items)
}

func (a *API) aveWalletTokens(ctx context.Context, wallet string) ([]map[string]any, error) {
	all := make([]map[string]any, 0)
	for page := 1; page <= 50; page++ {
		payload, err := a.aveJSON(ctx, "/v2api/walletinfo/v1/tokens", map[string]string{
			"user_address": strings.ToLower(wallet), "chain": "robinhood", "pageNO": strconv.Itoa(page), "pageSize": "40",
			"sort_dir": "desc", "sort": "last_txn_time", "is_self": "0", "hide_sold": "1", "hide_small": "1", "hide_risk": "1", "hide_noswap": "1",
		})
		if err != nil {
			return nil, err
		}
		data, _ := payload["data"].([]any)
		if len(data) == 0 {
			if nested, ok := payload["data"].(map[string]any); ok {
				data, _ = nested["data"].([]any)
			}
		}
		for _, raw := range data {
			if item, ok := raw.(map[string]any); ok {
				all = append(all, item)
			}
		}
		if len(data) < 40 {
			break
		}
	}
	return all, nil
}
func (a *API) tokenList(c *fiber.Ctx) error     { return a.tokens(c, true) }
func (a *API) tokenDisabled(c *fiber.Ctx) error { return a.tokens(c, false) }
func (a *API) tokens(c *fiber.Ctx, en bool) error {
	d := body(c)
	ts, e := a.Store.Tokens(c.Context(), id(d["userId"]), &en)
	if e != nil {
		return a.fail(c, e)
	}
	out := make([]map[string]any, 0, len(ts))
	for _, t := range ts {
		out = append(out, t.Map())
	}
	return a.ok(c, out)
}
func (a *API) tokenAdd(c *fiber.Ctx) error {
	d := body(c)
	u := id(d["userId"])
	if u <= 0 {
		return httpx.Error(c, 400, "userId is required")
	}
	if _, err := a.Store.User(c.Context(), u); err != nil {
		return httpx.Error(c, 404, "user not found")
	}
	tokenAddress, c0, c1 := low(str(d["tokenAddress"])), low(str(d["currency0"])), low(str(d["currency1"]))
	if c0 == "" || c1 == "" || c0 == c1 {
		return httpx.Error(c, 400, "PoolKey currencies must be different")
	}
	if c0 > c1 {
		return httpx.Error(c, 400, "PoolKey currency0 must sort before currency1")
	}
	if tokenAddress != c0 && tokenAddress != c1 {
		return httpx.Error(c, 400, "tokenAddress must be one of the PoolKey currencies")
	}
	poolID, pe := chain.PoolID(map[string]any{"currency0": c0, "currency1": c1, "fee": d["fee"], "tickSpacing": d["tickSpacing"], "hooks": d["hooks"]})
	if pe != nil || !strings.EqualFold(normalizePoolID(poolID), normalizePoolID(str(d["poolId"]))) {
		return httpx.Error(c, 400, "poolId does not match the supplied Uniswap V4 PoolKey")
	}
	pair := normalizePoolID(str(d["pair"]))
	if pair == "" {
		pair = normalizePoolID(poolID)
	}
	if !strings.EqualFold(pair, normalizePoolID(poolID)) {
		return httpx.Error(c, 400, "pair must match poolId")
	}
	quote := c1
	if tokenAddress == c1 {
		quote = c0
	}
	tokenDecimals := intPtr(d["decimals"])
	if tokenDecimals == nil && a.RPC != nil {
		if decimals, de := a.RPC.ERC20Decimals(c.Context(), tokenAddress); de == nil {
			tokenDecimals = &decimals
		}
	}
	totalSupplyRaw := strPtr(d["totalSupplyRaw"])
	if (totalSupplyRaw == nil || strings.TrimSpace(*totalSupplyRaw) == "") && a.RPC != nil {
		if supply, se := a.RPC.ERC20TotalSupply(c.Context(), tokenAddress); se == nil && supply != "" {
			totalSupplyRaw = &supply
		}
	}
	quoteDecimals := intPtr(d["quoteDecimals"])
	if quoteDecimals == nil && a.RPC != nil && !strings.EqualFold(quote, chain.NativeAddress) {
		if decimals, de := a.RPC.ERC20Decimals(c.Context(), quote); de == nil {
			quoteDecimals = &decimals
		}
	}
	quoteUSDPrice := strPtr(d["quoteUsdPrice"])
	if quoteUSDPrice == nil || strings.TrimSpace(*quoteUSDPrice) == "" {
		if strings.EqualFold(quote, chain.NativeAddress) && a.Cfg.NativeUSDPrice != "" {
			quoteUSDPrice = &a.Cfg.NativeUSDPrice
		} else {
			switch strings.ToUpper(str(d["quoteTokenSymbol"])) {
			case "USDG", "USDC", "USDT", "DAI":
				stableUSD := "1"
				quoteUSDPrice = &stableUSD
			}
		}
	}
	t := store.Token{UserID: u, TokenAddress: tokenAddress, CurveAddress: low(str(d["curveAddress"])), PoolID: normalizePoolID(poolID), Pair: pair, Amm: str(d["amm"]), Currency0: c0, Currency1: c1, Fee: int(id(d["fee"])), TickSpacing: int(id(d["tickSpacing"])), Hooks: low(str(d["hooks"])), QuoteTokenAddress: quote, Name: strPtr(d["tokenName"]), Symbol: strPtr(d["tokenSymbol"]), TokenLogoURL: strPtr(d["tokenLogoUrl"]), Decimals: tokenDecimals, TotalSupplyRaw: totalSupplyRaw, MarketCap: strPtr(d["marketCap"]), TokenPriceUsd: strPtr(d["tokenPriceUsd"]), QuoteDecimals: quoteDecimals, QuoteUSDPrice: quoteUSDPrice, Enabled: true}
	if v, ok := d["enabled"].(bool); ok {
		t.Enabled = v
	}
	if t.Pair == "" {
		t.Pair = t.PoolID
	}
	if t.TokenAddress == "" || t.CurveAddress == "" || t.PoolID == "" {
		return httpx.Error(c, 400, "tokenAddress, curveAddress and poolId are required")
	}
	x, e := a.Store.UpsertToken(c.Context(), t)
	if e != nil {
		return a.fail(c, e)
	}
	a.invalidateCache()
	return a.ok(c, x.Map())
}
func intPtr(v any) *int {
	if v == nil {
		return nil
	}
	n := int(id(v))
	return &n
}
func strPtr(v any) *string {
	if v == nil {
		return nil
	}
	s := str(v)
	return &s
}
func (a *API) tokenDelete(c *fiber.Ctx) error {
	d := body(c)
	e := a.Store.DeleteToken(c.Context(), id(d["userId"]), str(d["tokenAddress"]))
	if e != nil {
		return a.fail(c, e)
	}
	a.invalidateCache()
	return a.ok(c, map[string]any{"deleted": true})
}
func (a *API) tokenEnable(c *fiber.Ctx) error  { return a.setToken(c, true) }
func (a *API) tokenDisable(c *fiber.Ctx) error { return a.setToken(c, false) }
func (a *API) setToken(c *fiber.Ctx, en bool) error {
	d := body(c)
	e := a.Store.SetTokenEnabled(c.Context(), id(d["userId"]), str(d["tokenAddress"]), en)
	if e != nil {
		return a.fail(c, e)
	}
	a.invalidateCache()
	return a.ok(c, map[string]any{map[bool]string{true: "enabled", false: "disabled"}[en]: true})
}
func (a *API) syncTokens(c *fiber.Ctx) error {
	d := body(c)
	userID := id(d["userId"])
	if userID <= 0 {
		return httpx.Error(c, 400, "userId is required")
	}
	if _, e := a.Store.User(c.Context(), userID); e != nil {
		return httpx.Error(c, 404, "user not found")
	}
	enabledOnly := true
	ts, e := a.Store.Tokens(c.Context(), userID, &enabledOnly)
	if e != nil {
		return a.fail(c, e)
	}
	if a.Cfg.AveBaseURL == "" {
		return httpx.Error(c, 503, "AVE service is unavailable")
	}
	for _, token := range ts {
		detail, detailErr := a.aveJSON(c.Context(), "/v2api/token_info/v1/token/detail", map[string]string{"token_id": strings.ToLower(token.TokenAddress) + "-robinhood", "cache_use": "false"})
		if detailErr != nil {
			continue
		}
		data, _ := detail["data"].(map[string]any)
		pairs, _ := data["pairs"].([]any)
		for _, raw := range pairs {
			pair, ok := raw.(map[string]any)
			if !ok || !strings.EqualFold(pairAddress(pair, "token0_address", "token0Address"), token.TokenAddress) && !strings.EqualFold(pairAddress(pair, "token1_address", "token1Address"), token.TokenAddress) {
				continue
			}
			if token.QuoteTokenAddress != "" && !pairMatches(pair, token.TokenAddress, token.QuoteTokenAddress) && !strings.EqualFold(token.QuoteTokenAddress, chain.NativeAddress) {
				continue
			}
			if value := pairValue(pair, "market_cap"); value != nil {
				s := str(value)
				token.MarketCap = &s
			}
			priceKey := "token1_price_usd"
			if strings.EqualFold(str(pairValue(pair, "token0_address")), token.TokenAddress) {
				priceKey = "token0_price_usd"
			}
			if value := pairValue(pair, priceKey); value != nil {
				s := str(value)
				token.TokenPriceUsd = &s
			}
			if value := pairValue(pair, "buy_count_5m", "buys_tx_5m_count"); value != nil {
				v := id(value)
				token.BuyCount5m = &v
			}
			if value := pairValue(pair, "sell_count_5m", "sells_tx_5m_count"); value != nil {
				v := id(value)
				token.SellCount5m = &v
			}
			if value := pairValue(pair, "price_change_5m"); value != nil {
				s := str(value)
				token.PriceChange5m = &s
			}
			if value := pairValue(pair, "price_change_1h"); value != nil {
				s := str(value)
				token.PriceChange1h = &s
			}
			break
		}
		if (token.MarketCap == nil || strings.TrimSpace(*token.MarketCap) == "") && token.TotalSupplyRaw != nil && token.Decimals != nil && token.TokenPriceUsd != nil {
			if marketCap := derivedMarketCapFromSupply(*token.TotalSupplyRaw, *token.Decimals, *token.TokenPriceUsd); marketCap != nil {
				token.MarketCap = marketCap
			}
		}
		if _, upsertErr := a.Store.UpsertToken(c.Context(), token); upsertErr == nil {
			// Continue refreshing other tokens if one AVE item is malformed.
		}
	}
	a.invalidateCache()
	updated, _ := a.Store.Tokens(c.Context(), userID, &enabledOnly)
	return a.ok(c, map[string]any{"synced": true, "count": len(updated)})
}
func (a *API) aveList(c *fiber.Ctx) error {
	d := body(c)
	if id(d["userId"]) <= 0 {
		return httpx.Error(c, 400, "userId is required")
	}
	category, minMCP, maxMCP := str(d["category"]), str(d["minMcp"]), str(d["maxMcp"])
	createDay := id(d["createDay"])
	listRaw := map[string]any{}
	if userID := id(d["userId"]); userID > 0 {
		if u, userErr := a.Store.User(c.Context(), userID); userErr == nil {
			cfg := strategy.Parse(u.Config)
			if raw, ok := u.Config["listConfig"].(map[string]any); ok {
				listRaw = raw
			}
			if !strings.EqualFold(cfg.ListConfig.Source, "ave") {
				return a.ok(c, []any{})
			}
			if category == "" {
				category = cfg.ListConfig.Category
			}
			if minMCP == "" {
				minMCP = strconv.FormatFloat(cfg.ListConfig.MinMCP, 'f', -1, 64)
			}
			if maxMCP == "" {
				maxMCP = strconv.FormatFloat(cfg.ListConfig.MaxMCP, 'f', -1, 64)
			}
			if createDay == 0 {
				createDay = int64(cfg.ListConfig.CreateDay)
			}
		} else {
			return httpx.Error(c, 404, "user not found")
		}
	}
	if category == "" {
		category = "pons_out_hot"
	}
	params := map[string]string{"chain": "robinhood", "category": category, "pageNO": "1", "pageSize": "500", "sort": "created_at", "sort_dir": "desc", "marketcap_min": minMCP, "marketcap_max": maxMCP}
	// Preserve the extensible AVE options accepted by the Node service while
	// keeping explicit request fields authoritative.
	for key, queryKey := range map[string]string{"pageSize": "pageSize", "sort": "sort", "sortDir": "sort_dir", "amm": "amm", "selfAddress": "self_address", "createdAtMin": "created_at_min", "createdAtMax": "created_at_max"} {
		if _, explicit := d[key]; explicit {
			continue
		}
		if value := str(listRaw[key]); value != "" {
			params[queryKey] = value
		}
	}
	if params["marketcap_min"] == "" {
		params["marketcap_min"] = "20000"
	}
	if params["marketcap_max"] == "" {
		params["marketcap_max"] = "200000"
	}
	if createDay > 0 && params["created_at_min"] == "" {
		params["created_at_min"] = strconv.FormatInt(time.Now().Unix()-createDay*86400, 10)
	}
	if endDay := id(listRaw["endDay"]); endDay > 0 && params["created_at_min"] == "" && createDay == 0 {
		params["created_at_min"] = strconv.FormatInt(time.Now().Unix()-endDay*86400, 10)
	}
	if days := id(d["createDay"]); days > 0 && params["created_at_min"] == "" {
		params["created_at_min"] = strconv.FormatInt(time.Now().Unix()-days*86400, 10)
	}
	out, err := a.aveJSON(c.Context(), "/v1api/v4/tokens/treasure/list", params)
	if err != nil {
		return a.fail(c, err)
	}
	data, _ := out["data"].(map[string]any)
	items, _ := data["data"].([]any)
	views := make([]map[string]any, 0, len(items))
	for _, raw := range items {
		if item, ok := raw.(map[string]any); ok {
			views = append(views, aveListView(item))
		}
	}
	return a.ok(c, views)
}

func aveListView(item map[string]any) map[string]any {
	target := strings.ToLower(str(item["target_token"]))
	token0 := strings.ToLower(str(item["token0_address"]))
	token0Side := target != "" && target == token0
	tokenSymbol, tokenLogo := str(item["token1_symbol"]), str(item["token1_logo_url"])
	quoteSymbol, quoteLogo := str(item["token0_symbol"]), str(item["token0_logo_url"])
	if token0Side {
		tokenSymbol, tokenLogo = str(item["token0_symbol"]), str(item["token0_logo_url"])
		quoteSymbol, quoteLogo = str(item["token1_symbol"]), str(item["token1_logo_url"])
	}
	name := str(item["name_en"])
	if name == "" {
		name = str(item["name_zh"])
	}
	if name == "" {
		name = tokenSymbol
	}
	return map[string]any{
		"pair": item["pair"], "amm": item["amm"],
		"token0Address": item["token0_address"], "token0Symbol": item["token0_symbol"],
		"token1Address": item["token1_address"], "token1Symbol": item["token1_symbol"],
		"token0PriceUsd": aveNumber(item["token0_price_usd"]), "token1PriceUsd": aveNumber(item["token1_price_usd"]),
		"tokenAddress": item["target_token"], "tokenName": nullableString(name), "tokenSymbol": nullableString(tokenSymbol), "tokenLogoUrl": nullableString(tokenLogo),
		"quoteTokenSymbol": nullableString(quoteSymbol), "quoteTokenLogoUrl": nullableString(quoteLogo),
		"createdAt": item["created_at"], "lastTradeAt": item["last_trade_at"], "marketCap": item["market_cap"],
		"holdersCount": item["holders"], "buyCount5m": item["buys_tx_5m_count"], "sellCount5m": item["sells_tx_5m_count"],
		"priceChange5m": item["price_change_5m"], "priceChange1h": item["price_change_1h"],
		"buyTax":  firstAny(item, "buy_tax", "buyTax", "buy_tax_rate", "buyTaxRate", "buy_tax_percent", "buyTaxPercent"),
		"sellTax": firstAny(item, "sell_tax", "sellTax", "sell_tax_rate", "sellTaxRate", "sell_tax_percent", "sellTaxPercent"),
		"action":  "START",
	}
}

func aveNumber(v any) any {
	if n, ok := v.(float64); ok {
		return n
	}
	return nil
}

func nullableString(v string) any {
	if v == "" {
		return nil
	}
	return v
}

func firstAny(m map[string]any, keys ...string) any {
	for _, key := range keys {
		if v, ok := m[key]; ok && v != nil {
			if s, ok := v.(string); ok && s == "" {
				continue
			}
			return v
		}
	}
	return nil
}

func aveValue(sources []map[string]any, keys ...string) any {
	for _, source := range sources {
		if source == nil {
			continue
		}
		if value := pairValue(source, keys...); value != nil && strings.TrimSpace(str(value)) != "" {
			return value
		}
	}
	return nil
}

func aveStringPtr(sources []map[string]any, keys ...string) *string {
	value := aveValue(sources, keys...)
	if value == nil {
		return nil
	}
	text := strings.TrimSpace(str(value))
	if text == "" {
		return nil
	}
	return &text
}

func aveInt64Ptr(sources []map[string]any, keys ...string) *int64 {
	value := aveValue(sources, keys...)
	if value == nil {
		return nil
	}
	n := id(value)
	return &n
}

func decimalRat(value any) (*big.Rat, bool) {
	s := strings.TrimSpace(str(value))
	if s == "" {
		return nil, false
	}
	if r, ok := new(big.Rat).SetString(s); ok {
		return r, true
	}
	f, ok := new(big.Float).SetString(s)
	if !ok {
		return nil, false
	}
	r, _ := f.Rat(nil)
	return r, r != nil
}

func formatDecimalRat(value *big.Rat) string {
	if value == nil {
		return ""
	}
	text := value.FloatString(18)
	text = strings.TrimRight(strings.TrimRight(text, "0"), ".")
	if text == "" || text == "-0" {
		return "0"
	}
	return text
}

func derivedMarketCapFromSupply(totalSupplyRaw string, decimals int, tokenPriceUSD any) *string {
	supply, ok := new(big.Int).SetString(strings.TrimSpace(totalSupplyRaw), 10)
	if !ok || supply.Sign() <= 0 || decimals < 0 || decimals > 255 {
		return nil
	}
	price, ok := decimalRat(tokenPriceUSD)
	if !ok || price.Sign() < 0 {
		return nil
	}
	denominator := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	result := new(big.Rat).SetFrac(new(big.Int).Mul(supply, price.Num()), new(big.Int).Mul(denominator, price.Denom()))
	text := formatDecimalRat(result)
	if text == "" {
		return nil
	}
	return &text
}

func derivedMarketCapFromAve(sources []map[string]any) *string {
	total := aveValue(sources, "total", "total_supply", "totalSupply")
	price := aveValue(sources, "current_price_usd", "currentPriceUsd")
	totalRat, totalOK := decimalRat(total)
	priceRat, priceOK := decimalRat(price)
	if !totalOK || !priceOK || totalRat.Sign() <= 0 || priceRat.Sign() < 0 {
		return nil
	}
	value := new(big.Rat).Mul(totalRat, priceRat)
	text := formatDecimalRat(value)
	if text == "" {
		return nil
	}
	return &text
}

func (a *API) startAve(c *fiber.Ctx) error {
	d := body(c)
	target := low(str(d["targetToken"]))
	if target == "" {
		target = low(str(d["tokenAddress"]))
	}
	if target == "" {
		return httpx.Error(c, 400, "targetToken or tokenAddress is required")
	}
	detail, err := a.aveJSON(c.Context(), "/v2api/token_info/v1/token/detail", map[string]string{"token_id": target + "-robinhood", "cache_use": "false"})
	if err != nil {
		return a.fail(c, err)
	}
	extra, err := a.aveJSON(c.Context(), "/v1api/v2/tokens/"+target+"-robinhood/extraDetail", nil)
	if err != nil {
		return a.fail(c, err)
	}
	extraData := extra
	if nested, ok := extra["data"].(map[string]any); ok {
		extraData = nested
	}
	data, _ := detail["data"].(map[string]any)
	tokenDetail, _ := data["token"].(map[string]any)
	pairs, _ := data["pairs"].([]any)
	requested := normalizePoolID(str(d["pair"]))
	var pair map[string]any
	for _, raw := range pairs {
		p, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if !strings.EqualFold(str(p["target_token"]), target) {
			continue
		}
		if requested != "" && !strings.EqualFold(normalizePoolID(str(p["pair"])), requested) {
			continue
		}
		pair = p
		break
	}
	if pair == nil {
		return httpx.Error(c, 404, "AVE token pair not found for the target token")
	}
	factory := a.Cfg.PonsFactory
	launched, err := a.RPC.LaunchedToken(c.Context(), factory, target)
	if err != nil {
		return a.fail(c, err)
	}
	if !launched.Exists || launched.Curve == (common.Address{}) || launched.Curve == common.HexToAddress(chain.NativeAddress) {
		return httpx.Error(c, 400, "AVE token has no Pons V2 launch record")
	}
	pairToken := low(launched.PairToken.Hex())
	c0, c1 := target, low(pairToken)
	if c1 < c0 {
		c0, c1 = c1, c0
	}
	poolID, err := chain.PoolID(map[string]any{"currency0": c0, "currency1": c1, "fee": launched.PoolFee, "tickSpacing": launched.TickSpacing, "hooks": a.Cfg.PonsExternalHooks})
	if err != nil {
		return a.fail(c, err)
	}
	if requested != "" && !strings.EqualFold(requested, normalizePoolID(poolID)) {
		return httpx.Error(c, 400, "poolId does not match the supplied Uniswap V4 PoolKey")
	}
	d["tokenAddress"], d["curveAddress"], d["poolId"], d["pair"] = target, low(launched.Curve.Hex()), normalizePoolID(poolID), normalizePoolID(poolID)
	d["currency0"], d["currency1"], d["fee"], d["tickSpacing"], d["hooks"] = c0, c1, launched.PoolFee, launched.TickSpacing, a.Cfg.PonsExternalHooks
	// The factory is authoritative for the quote currency. AVE may represent
	// native ETH as its sentinel address (0xeeee...), while the chain PoolKey
	// uses address(0).
	d["quoteTokenAddress"] = pairToken
	if str(d["tokenSymbol"]) == "" {
		if strings.EqualFold(str(pair["token0_address"]), target) {
			d["tokenSymbol"] = pair["token0_symbol"]
		} else {
			d["tokenSymbol"] = pair["token1_symbol"]
		}
	}
	if str(d["tokenName"]) == "" {
		d["tokenName"] = d["tokenSymbol"]
	}
	if str(d["amm"]) == "" {
		d["amm"] = pair["amm"]
	}
	if str(d["quoteTokenSymbol"]) == "" {
		if strings.EqualFold(str(pair["token0_address"]), target) {
			d["quoteTokenSymbol"] = pair["token1_symbol"]
		} else {
			d["quoteTokenSymbol"] = pair["token0_symbol"]
		}
	}
	if str(d["tokenLogoUrl"]) == "" {
		if strings.EqualFold(str(pair["token0_address"]), target) {
			d["tokenLogoUrl"] = pair["token0_logo_url"]
		} else {
			d["tokenLogoUrl"] = pair["token1_logo_url"]
		}
	}
	if str(d["quoteTokenLogoUrl"]) == "" {
		if strings.EqualFold(str(pair["token0_address"]), target) {
			d["quoteTokenLogoUrl"] = pair["token1_logo_url"]
		} else {
			d["quoteTokenLogoUrl"] = pair["token0_logo_url"]
		}
	}
	// AVE spreads token metrics across the selected pair, token detail and
	// extraDetail responses. Use the pair first, then fill missing fields from
	// the token-level responses so startAveToken stores the same data shown by
	// the Node client.
	// AVE puts token-level values below data.token. Include that object before
	// the response wrapper so fields such as total/current_price_usd are found.
	sources := []map[string]any{pair, extraData, tokenDetail, data}
	taxSources := []map[string]any{extraData, pair, tokenDetail, data}
	buyTax := aveTaxSources(taxSources, "buy_tax", "buyTax", "buy_tax_rate", "buyTaxRate", "buy_tax_percent", "buyTaxPercent")
	sellTax := aveTaxSources(taxSources, "sell_tax", "sellTax", "sell_tax_rate", "sellTaxRate", "sell_tax_percent", "sellTaxPercent")
	if value := aveValue(sources, "token_name", "tokenName", "name_en", "name_zh", "name"); str(d["tokenName"]) == "" && value != nil {
		d["tokenName"] = value
	}
	if value := aveValue(sources, "symbol", "token_symbol", "tokenSymbol"); str(d["tokenSymbol"]) == "" && value != nil {
		d["tokenSymbol"] = value
	}
	if value := aveValue(sources, "token_logo_url", "tokenLogoUrl", "logo_url", "logo"); str(d["tokenLogoUrl"]) == "" && value != nil {
		d["tokenLogoUrl"] = value
	}
	if value := aveValue(sources, "quote_token_symbol", "quoteTokenSymbol"); str(d["quoteTokenSymbol"]) == "" && value != nil {
		d["quoteTokenSymbol"] = value
	}
	if value := aveValue(sources, "quote_token_logo_url", "quoteTokenLogoUrl"); str(d["quoteTokenLogoUrl"]) == "" && value != nil {
		d["quoteTokenLogoUrl"] = value
	}
	b, _ := json.Marshal(d)
	c.Request().SetBody(b)
	// Upsert directly so route discovery can fail before an HTTP success body
	// is committed. The Node service disables the monitor and returns an error
	// when no executable native route exists.
	quoteDecimals := 18
	tokenDecimals := 0
	var tokenDecimalsPtr *int
	if a.RPC != nil {
		if decimals, de := a.RPC.ERC20Decimals(c.Context(), target); de == nil {
			tokenDecimals = decimals
			tokenDecimalsPtr = &tokenDecimals
		}
	}
	if a.RPC != nil && pairToken != chain.NativeAddress && pairToken != wethAddress {
		if decimals, de := a.RPC.ERC20Decimals(c.Context(), pairToken); de == nil {
			quoteDecimals = decimals
		}
	}
	quoteUSD := ""
	if pairToken == chain.NativeAddress || pairToken == wethAddress {
		quoteUSD = a.Cfg.NativeUSDPrice
	} else if pairToken == usdgAddress {
		quoteUSD = "1"
	}
	targetIsToken0 := strings.EqualFold(str(pair["token0_address"]), target)
	var targetPriceEth, targetPriceUSD any
	if targetIsToken0 {
		targetPriceEth = pairValue(pair, "token0_price_eth")
		targetPriceUSD = pairValue(pair, "token0_price_usd")
	} else {
		targetPriceEth = pairValue(pair, "token1_price_eth")
		targetPriceUSD = pairValue(pair, "token1_price_usd")
	}
	if targetPriceEth == nil {
		targetPriceEth = aveValue(sources, "token_price_eth", "tokenPriceEth", "price_eth", "priceEth")
	}
	if targetPriceUSD == nil {
		targetPriceUSD = aveValue(sources, "token_price_usd", "tokenPriceUsd", "price_usd", "priceUsd", "current_price_usd", "currentPriceUsd", "price")
	}
	var quotePriceUSD any
	if targetIsToken0 {
		quotePriceUSD = pairValue(pair, "token1_price_usd")
	} else {
		quotePriceUSD = pairValue(pair, "token0_price_usd")
	}
	if tokenDecimalsPtr == nil {
		key := "token1_decimal"
		if targetIsToken0 {
			key = "token0_decimal"
		}
		if decimals := id(pairValue(pair, key)); decimals > 0 {
			tokenDecimals = int(decimals)
			tokenDecimalsPtr = &tokenDecimals
		}
	}
	var totalSupplyRaw *string
	if a.RPC != nil {
		if raw, supplyErr := a.RPC.ERC20TotalSupply(c.Context(), target); supplyErr == nil && raw != "" {
			totalSupplyRaw = &raw
		}
	}
	if totalSupplyRaw == nil {
		totalSupplyRaw = aveStringPtr(sources, "total_supply_raw", "totalSupplyRaw", "total_supply", "totalSupply", "token_supply", "supply")
	}
	marketCap := aveStringPtr(sources, "market_cap", "marketCap", "marketcap", "market_cap_usd", "mcap")
	if marketCap == nil {
		marketCap = derivedMarketCapFromAve(sources)
	}
	if marketCap == nil && totalSupplyRaw != nil && tokenDecimalsPtr != nil {
		marketCap = derivedMarketCapFromSupply(*totalSupplyRaw, *tokenDecimalsPtr, targetPriceUSD)
	}
	volume5m := aveStringPtr(sources, "volume_u_5m", "volume_5m", "volume5m", "volume_usd_5m")
	buyCount5m := aveInt64Ptr(sources, "buys_tx_5m_count", "buy_count_5m", "buyCount5m")
	sellCount5m := aveInt64Ptr(sources, "sells_tx_5m_count", "sell_count_5m", "sellCount5m")
	tokenPriceEth := aveStringPtr([]map[string]any{{"value": targetPriceEth}}, "value")
	tokenPriceUSD := aveStringPtr([]map[string]any{{"value": targetPriceUSD}}, "value")
	t := store.Token{UserID: id(d["userId"]), TokenAddress: target, CurveAddress: low(str(d["curveAddress"])), PoolID: normalizePoolID(poolID), Pair: normalizePoolID(poolID), Amm: str(d["amm"]), Currency0: c0, Currency1: c1, Fee: int(bigIntToInt64(launched.PoolFee)), TickSpacing: int(bigIntToInt64(launched.TickSpacing)), Hooks: low(a.Cfg.PonsExternalHooks), QuoteTokenAddress: pairToken, Name: strPtr(d["tokenName"]), Symbol: strPtr(d["tokenSymbol"]), TokenLogoURL: strPtr(d["tokenLogoUrl"]), QuoteTokenSymbol: strPtr(d["quoteTokenSymbol"]), QuoteTokenLogoURL: strPtr(d["quoteTokenLogoUrl"]), Decimals: tokenDecimalsPtr, TotalSupplyRaw: totalSupplyRaw, MarketCap: marketCap, TokenPriceEth: tokenPriceEth, TokenPriceUsd: tokenPriceUSD, PriceChange5m: aveStringPtr(sources, "price_change_5m", "priceChange5m", "price_change_5min"), PriceChange1h: aveStringPtr(sources, "price_change_1h", "priceChange1h"), PriceChange24h: aveStringPtr(sources, "price_change_24h", "priceChange24h"), HoldersCount: aveInt64Ptr(sources, "holders_count", "holdersCount", "holder_count", "holders"), BuyCount5m: buyCount5m, SellCount5m: sellCount5m, Volume5m: volume5m, QuoteDecimals: &quoteDecimals, QuoteUSDPrice: func() *string {
		if quoteUSD != "" {
			return strPtr(quoteUSD)
		}
		return strPtr(quotePriceUSD)
	}(), BuyTax: buyTax, SellTax: sellTax, Enabled: true}
	if v, ok := d["enabled"].(bool); ok {
		t.Enabled = v
	}
	if _, err := a.Store.UpsertToken(c.Context(), t); err != nil {
		return a.fail(c, err)
	}
	a.invalidateCache()
	targetHop := routeHop{PoolID: normalizePoolID(poolID), Currency0: c0, Currency1: c1, Fee: bigIntToInt64(launched.PoolFee), TickSpacing: bigIntToInt64(launched.TickSpacing), Hooks: low(a.Cfg.PonsExternalHooks), TokenIn: pairToken, TokenOut: target, HookData: "0x"}
	hops, ok := a.discoverNativeRoute(c.Context(), id(d["userId"]), target, normalizePoolID(poolID), pairToken, targetHop)
	if !ok {
		_ = a.Store.SetTokenEnabled(c.Context(), id(d["userId"]), target, false)
		a.invalidateCache()
		return httpx.Error(c, 400, "No executable Uniswap V4 route from native ETH to the token quote currency")
	}
	if err := a.persistDiscoveredRoute(c.Context(), id(d["userId"]), target, normalizePoolID(poolID), hops); err != nil {
		return a.fail(c, err)
	}
	tracked, err := a.Store.Token(c.Context(), id(d["userId"]), target, low(str(d["curveAddress"])))
	if err != nil {
		return a.fail(c, err)
	}
	return a.ok(c, tracked.Map())
}
func (a *API) routeList(c *fiber.Ctx) error {
	d := body(c)
	v, e := a.Store.Routes(c.Context(), id(d["userId"]))
	if e != nil {
		return a.fail(c, e)
	}
	return a.ok(c, v)
}
func (a *API) routeSave(c *fiber.Ctx) error {
	d := body(c)
	userID, targetPool, source, target := id(d["userId"]), normalizePoolID(str(d["targetPoolId"])), low(str(d["sourceToken"])), low(str(d["targetToken"]))
	if userID <= 0 {
		return httpx.Error(c, 400, "userId is required")
	}
	if _, err := a.Store.User(c.Context(), userID); err != nil {
		return httpx.Error(c, 404, "user not found")
	}
	hops, ok := d["hops"].([]any)
	if !ok || len(hops) < 1 || len(hops) > 3 {
		return httpx.Error(c, 400, "a monitor route must contain between 1 and 3 hops")
	}
	if source != chain.NativeAddress {
		return httpx.Error(c, 400, "only native ETH wallet routes are supported currently")
	}
	if str(d["direction"]) != "" && !strings.EqualFold(str(d["direction"]), "buy") {
		return httpx.Error(c, 400, "monitor route source must be native ETH")
	}
	normalized := make([]any, len(hops))
	current := source
	for i, raw := range hops {
		h, valid := raw.(map[string]any)
		if !valid {
			return httpx.Error(c, 400, fmt.Sprintf("invalid route hop %d", i+1))
		}
		tokenIn, tokenOut := low(str(h["tokenIn"])), low(str(h["tokenOut"]))
		if tokenIn == "" {
			tokenIn = current
		}
		if tokenIn != current || tokenOut == "" || tokenIn == tokenOut {
			return httpx.Error(c, 400, fmt.Sprintf("route hop %d is not connected to the previous hop", i+1))
		}
		c0, c1 := low(str(h["currency0"])), low(str(h["currency1"]))
		if c0 == "" || c1 == "" || c0 >= c1 {
			return httpx.Error(c, 400, fmt.Sprintf("route hop %d has an invalid PoolKey order", i+1))
		}
		poolID, pe := chain.PoolID(map[string]any{"currency0": c0, "currency1": c1, "fee": h["fee"], "tickSpacing": h["tickSpacing"], "hooks": h["hooks"]})
		if pe != nil || !strings.EqualFold(normalizePoolID(str(h["poolId"])), normalizePoolID(poolID)) {
			return httpx.Error(c, 400, fmt.Sprintf("route hop %d poolId does not match its PoolKey", i+1))
		}
		normalized[i] = map[string]any{"poolId": normalizePoolID(poolID), "currency0": c0, "currency1": c1, "fee": h["fee"], "tickSpacing": h["tickSpacing"], "hooks": low(str(h["hooks"])), "tokenIn": tokenIn, "tokenOut": tokenOut, "hookData": str(h["hookData"])}
		current = tokenOut
	}
	if current != target || !strings.EqualFold(str(normalized[len(normalized)-1].(map[string]any)["poolId"]), targetPool) {
		return httpx.Error(c, 400, "the final route output or targetPoolId is invalid")
	}
	v, e := a.Store.SaveRoute(c.Context(), userID, targetPool, source, target, "buy", normalized, target)
	var sellValue map[string]any
	if e == nil {
		reversed := make([]any, len(normalized))
		for i := range normalized {
			h := normalized[len(normalized)-1-i].(map[string]any)
			reversed[i] = map[string]any{"poolId": h["poolId"], "currency0": h["currency0"], "currency1": h["currency1"], "fee": h["fee"], "tickSpacing": h["tickSpacing"], "hooks": h["hooks"], "tokenIn": h["tokenOut"], "tokenOut": h["tokenIn"], "hookData": h["hookData"]}
		}
		sellValue, e = a.Store.SaveRoute(c.Context(), userID, targetPool, target, source, "sell", reversed, target)
	}
	if e != nil {
		return a.fail(c, e)
	}
	return a.ok(c, map[string]any{"buy": v, "sell": sellValue})
}
func (a *API) routeDelete(c *fiber.Ctx) error {
	d := body(c)
	if id(d["userId"]) <= 0 {
		return httpx.Error(c, 400, "userId is required")
	}
	e := a.Store.DeleteRoute(c.Context(), id(d["userId"]), normalizePoolID(str(d["targetPoolId"])), low(str(d["sourceToken"])), low(str(d["targetToken"])))
	if e != nil {
		return a.fail(c, e)
	}
	return a.ok(c, map[string]any{"disabled": true})
}
func (a *API) quote(c *fiber.Ctx) error {
	v, e := a.Trading.Quote(c.Context(), body(c))
	if e != nil {
		return a.fail(c, e)
	}
	return a.ok(c, v)
}
func (a *API) ponsQuote(c *fiber.Ctx) error { return a.quote(c) }
func (a *API) ponsBuy(c *fiber.Ctx) error   { return a.ponsSwap(c, true) }
func (a *API) ponsSell(c *fiber.Ctx) error  { return a.ponsSwap(c, false) }
func (a *API) ponsSwap(c *fiber.Ctx, buy bool) error {
	d := body(c)
	curve, token := str(d["curveAddress"]), str(d["tokenAddress"])
	if curve == "" || token == "" {
		return httpx.Error(c, 400, "curveAddress and tokenAddress are required")
	}
	recipient := str(d["recipient"])
	if recipient == "" {
		recipient = str(d["to"])
	}
	if recipient == "" {
		recipient = "0x0000000000000000000000000000000000000000"
	}
	amount := str(d["quoteInRaw"])
	if !buy {
		amount = str(d["tokensInRaw"])
	}
	if amount == "" {
		return httpx.Error(c, 400, "amount is required")
	}
	state, e := a.RPC.PonsState(c.Context(), curve, recipient)
	if e != nil {
		return a.fail(c, e)
	}
	in := chain.ToBig(amount)
	if in.Sign() < 0 || in.Sign() == 0 {
		return httpx.Error(c, 400, "amount must be a positive integer in raw units")
	}
	slip := int64(100)
	if v, ok := d["slippageBps"]; ok {
		slip = chain.ToBig(v).Int64()
	}
	if slip < 0 || slip > 5000 {
		return httpx.Error(c, 400, "slippageBps must be between 0 and 5000")
	}
	var out, spent, refund *big.Int
	clamped := false
	if buy {
		out, spent, refund, clamped = chain.PonsBuyQuote(state, in)
	} else {
		out = chain.PonsSellQuote(state, in)
		spent = in
		refund = big.NewInt(0)
	}
	min := new(big.Int).Div(new(big.Int).Mul(out, new(big.Int).Sub(big.NewInt(10000), big.NewInt(slip))), big.NewInt(10000))
	data, e := func() ([]byte, error) {
		if buy {
			return a.RPC.PackPonsBuy(in, min, recipient)
		}
		return a.RPC.PackPonsSell(in, min, recipient)
	}()
	if e != nil {
		return a.fail(c, e)
	}
	if key := str(d["privateKey"]); key != "" && !a.Trading.DryRun {
		exec := map[string]any{"curveAddress": curve, "tokenAddress": token, "recipient": recipient, "privateKey": key, "amountRaw": in.String(), "slippageBps": slip}
		v, te := a.Trading.PonsSwap(c.Context(), exec, buy)
		if te != nil {
			return a.fail(c, te)
		}
		return a.ok(c, v)
	}
	if a.Trading.DryRun {
		return a.ok(c, map[string]any{"mode": "dry-run", "side": map[bool]string{true: "buy", false: "sell"}[buy], "wallet": strings.ToLower(recipient), "recipient": strings.ToLower(recipient), "curveAddress": strings.ToLower(curve), "tokenAddress": strings.ToLower(token), "pairToken": strings.ToLower(state.PairToken.Hex()), "quoteInRaw": map[bool]string{true: in.String(), false: ""}[buy], "tokensInRaw": map[bool]string{true: "", false: in.String()}[buy], "expectedTokensOutRaw": map[bool]string{true: out.String(), false: ""}[buy], "expectedQuoteOutRaw": map[bool]string{true: "", false: out.String()}[buy], "minimumAmountOutRaw": min.String(), "spentRaw": spent.String(), "refundRaw": refund.String(), "clamped": clamped, "transactionHash": nil, "to": curve, "data": "0x" + hex.EncodeToString(data)})
	}
	return a.ok(c, map[string]any{"mode": "calldata-preview", "side": map[bool]string{true: "buy", false: "sell"}[buy], "to": curve, "data": "0x" + hex.EncodeToString(data), "valueRaw": map[bool]string{true: in.String(), false: "0"}[buy], "amountOutMinimumRaw": min.String(), "expectedAmountOutRaw": out.String(), "recipient": strings.ToLower(recipient)})
}
func (a *API) swap(c *fiber.Ctx) error {
	d := body(c)
	if id(d["userId"]) > 0 && str(d["token"]) != "" {
		var e error
		d, e = a.buildMonitoredSwap(c, d)
		if e != nil {
			return a.fail(c, e)
		}
	}
	v, e := a.Trading.Swap(c.Context(), d)
	if e != nil {
		return a.fail(c, e)
	}
	return a.ok(c, v)
}

func (a *API) buildMonitoredSwap(c *fiber.Ctx, in map[string]any) (map[string]any, error) {
	uid := id(in["userId"])
	token := low(str(in["token"]))
	direction := strings.ToLower(str(in["direction"]))
	if direction == "" {
		if str(in["ethAmount"]) != "" {
			direction = "buy"
		} else {
			direction = "sell"
		}
	}
	u, err := a.Store.User(c.Context(), uid)
	if err != nil {
		return nil, fmt.Errorf("monitor user not found")
	}
	if u.PrivateKeyEncrypted == nil {
		return nil, fmt.Errorf("monitor user private key not found")
	}
	key, err := secret.Decrypt(*u.PrivateKeyEncrypted, a.Cfg.EncryptionKey)
	if err != nil {
		return nil, err
	}
	routes, err := a.Store.Routes(c.Context(), uid)
	if err != nil {
		return nil, err
	}
	var route map[string]any
	for _, r := range routes {
		if strings.EqualFold(str(r["direction"]), direction) && (token == "" || strings.EqualFold(str(r["targetToken"]), token) || strings.EqualFold(str(r["destinationToken"]), token)) {
			route = r
			break
		}
	}
	if route == nil {
		return nil, fmt.Errorf("no cached %s route found for this monitored token", direction)
	}
	hops, ok := route["hops"].([]any)
	if !ok {
		return nil, fmt.Errorf("cached route hops are invalid")
	}
	path := make([]any, 0, len(hops))
	firstInput := str(route["sourceToken"])
	for _, raw := range hops {
		h, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		intermediate := h["tokenOut"]
		if intermediate == nil {
			intermediate = h["intermediateCurrency"]
		}
		if firstInput == "" || strings.EqualFold(firstInput, chain.NativeAddress) {
			if h["tokenIn"] != nil {
				firstInput = str(h["tokenIn"])
			}
		}
		path = append(path, map[string]any{"intermediateCurrency": intermediate, "fee": h["fee"], "tickSpacing": h["tickSpacing"], "hooks": h["hooks"], "hookData": h["hookData"]})
	}
	amount := str(in["amountInRaw"])
	if direction == "buy" && amount == "" {
		amount = parseEtherRaw(str(in["ethAmount"]))
	}
	if direction == "sell" && amount == "" {
		balance, be := a.RPC.ERC20Balance(c.Context(), token, u.WalletAddress)
		if be != nil {
			return nil, be
		}
		percent := chain.ToBig(in["percent"])
		if percent.Sign() <= 0 || percent.Cmp(big.NewInt(100)) > 0 {
			return nil, fmt.Errorf("percent must be between 1 and 100")
		}
		amount = new(big.Int).Div(new(big.Int).Mul(balance, percent), big.NewInt(100)).String()
	}
	if amount == "" || amount == "0" {
		return nil, fmt.Errorf("amount is zero")
	}
	wrapNative := direction == "buy" && !strings.EqualFold(firstInput, str(route["sourceToken"]))
	out := map[string]any{"currencyIn": firstInput, "path": path, "amountInRaw": amount, "amountOutMinimumRaw": str(in["amountOutMinimumRaw"]), "recipient": u.WalletAddress, "privateKey": key, "wrapNative": wrapNative, "unwrapNative": direction == "sell" && strings.EqualFold(str(route["destinationToken"]), chain.NativeAddress)}
	if out["amountOutMinimumRaw"] == "" {
		out["amountOutMinimumRaw"] = "0"
	}
	return out, nil
}
func parseEtherRaw(s string) string {
	if s == "" {
		return ""
	}
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		return ""
	}
	n := new(big.Int).Mul(r.Num(), new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	return n.Quo(n, r.Denom()).String()
}
func (a *API) aveRequest(c *fiber.Ctx, path string, q map[string]string) error {
	v, err := a.aveJSON(c.Context(), path, q)
	if err != nil {
		return httpx.Error(c, 503, err.Error())
	}
	return a.ok(c, v)
}
func (a *API) fileUpload(c *fiber.Ctx) error {
	f, e := c.FormFile("file")
	if e != nil {
		// upload-multiple uses the conventional `files` field; accept its first
		// part here so the endpoint remains useful with clients that send one file.
		if form, fe := c.MultipartForm(); fe == nil && len(form.File["files"]) > 0 {
			f = form.File["files"][0]
		} else {
			return httpx.Error(c, 400, "file is required")
		}
	}
	if f.Size > a.Cfg.MaxPhotoSizeMB*1024*1024 {
		return httpx.Error(c, 400, "file too large")
	}
	allowed := false
	for _, typ := range strings.Split(a.Cfg.AllowedPhotoTypes, ",") {
		if strings.EqualFold(strings.TrimSpace(typ), f.Header.Get("Content-Type")) {
			allowed = true
			break
		}
	}
	if !allowed {
		return httpx.Error(c, 400, "file type not allowed")
	}
	isPublic := strings.EqualFold(c.FormValue("isPublic"), "true")
	name := uuid.NewString() + filepath.Ext(f.Filename)
	os.MkdirAll(a.Cfg.StoragePath, 0755)
	if e = c.SaveFile(f, filepath.Join(a.Cfg.StoragePath, name)); e != nil {
		return a.fail(c, e)
	}
	_, e = a.Store.DB.Exec(c.Context(), "INSERT INTO files(filename,original_name,mime_type,size,driver,path,url,is_public) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", name, f.Filename, f.Header.Get("Content-Type"), f.Size, a.Cfg.StorageDriver, name, "/uploads/"+name, isPublic)
	if e != nil {
		return a.fail(c, e)
	}
	return a.ok(c, map[string]any{"filename": name, "originalName": f.Filename, "mimeType": f.Header.Get("Content-Type"), "size": f.Size, "driver": a.Cfg.StorageDriver, "url": "/uploads/" + name, "isPublic": isPublic})
}

func (a *API) fileUploadMultiple(c *fiber.Ctx) error {
	form, err := c.MultipartForm()
	if err != nil || form == nil || len(form.File["files"]) == 0 {
		return httpx.Error(c, 400, "files are required")
	}
	results := make([]any, 0, len(form.File["files"]))
	for _, fh := range form.File["files"] {
		// Reuse the single-file validation and persistence path by temporarily
		// exposing the current header under the `file` field.
		if fh.Size > a.Cfg.MaxPhotoSizeMB*1024*1024 {
			continue
		}
		allowed := false
		for _, typ := range strings.Split(a.Cfg.AllowedPhotoTypes, ",") {
			if strings.EqualFold(strings.TrimSpace(typ), fh.Header.Get("Content-Type")) {
				allowed = true
				break
			}
		}
		if !allowed {
			continue
		}
		name := uuid.NewString() + filepath.Ext(fh.Filename)
		if err := os.MkdirAll(a.Cfg.StoragePath, 0755); err != nil {
			return a.fail(c, err)
		}
		if err := c.SaveFile(fh, filepath.Join(a.Cfg.StoragePath, name)); err != nil {
			return a.fail(c, err)
		}
		pub := strings.EqualFold(c.FormValue("isPublic"), "true")
		_, err = a.Store.DB.Exec(c.Context(), "INSERT INTO files(filename,original_name,mime_type,size,driver,path,url,is_public) VALUES($1,$2,$3,$4,$5,$6,$7,$8)", name, fh.Filename, fh.Header.Get("Content-Type"), fh.Size, a.Cfg.StorageDriver, name, "/uploads/"+name, pub)
		if err != nil {
			return a.fail(c, err)
		}
		results = append(results, map[string]any{"filename": name, "originalName": fh.Filename, "mimeType": fh.Header.Get("Content-Type"), "size": fh.Size, "driver": a.Cfg.StorageDriver, "url": "/uploads/" + name, "isPublic": pub})
	}
	return a.ok(c, results)
}
func (a *API) fileList(c *fiber.Ctx) error {
	page, _ := strconv.Atoi(c.Query("page", "1"))
	limit, _ := strconv.Atoi(c.Query("limit", "10"))
	if page < 1 {
		page = 1
	}
	if limit < 1 {
		limit = 10
	}
	if limit > 100 {
		limit = 100
	}
	var total int
	_ = a.Store.DB.QueryRow(c.Context(), "SELECT COUNT(*) FROM files WHERE deleted_at IS NULL").Scan(&total)
	rows, e := a.Store.DB.Query(c.Context(), "SELECT id,filename,original_name,mime_type,size,driver,url,is_public,created_at,updated_at FROM files WHERE deleted_at IS NULL ORDER BY created_at DESC LIMIT $1 OFFSET $2", limit, (page-1)*limit)
	if e != nil {
		return a.fail(c, e)
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id int64
		var fn, on, mt, dr string
		var sz int64
		var url *string
		var pub bool
		var cr, up time.Time
		_ = rows.Scan(&id, &fn, &on, &mt, &sz, &dr, &url, &pub, &cr, &up)
		out = append(out, map[string]any{"id": id, "filename": fn, "originalName": on, "mimeType": mt, "size": sz, "driver": dr, "url": url, "isPublic": pub, "createdAt": cr, "updatedAt": up})
	}
	totalPages := (total + limit - 1) / limit
	return a.ok(c, map[string]any{"files": out, "total": total, "page": page, "limit": limit, "totalPages": totalPages})
}
func (a *API) fileGet(c *fiber.Ctx) error {
	var v map[string]any
	var id int64
	id, _ = strconv.ParseInt(c.Params("id"), 10, 64)
	var fn, on, mt, dr, path string
	var sz int64
	var url *string
	var pub bool
	var cr, up time.Time
	e := a.Store.DB.QueryRow(c.Context(), "SELECT id,filename,original_name,mime_type,size,driver,path,url,is_public,created_at,updated_at FROM files WHERE id=$1 AND deleted_at IS NULL", id).Scan(&id, &fn, &on, &mt, &sz, &dr, &path, &url, &pub, &cr, &up)
	if e != nil {
		return httpx.Error(c, 404, "file not found")
	}
	v = map[string]any{"id": id, "filename": fn, "originalName": on, "mimeType": mt, "size": sz, "driver": dr, "path": path, "url": url, "isPublic": pub, "createdAt": cr, "updatedAt": up}
	return a.ok(c, v)
}
func (a *API) fileDelete(c *fiber.Ctx) error {
	id, _ := strconv.ParseInt(c.Params("id"), 10, 64)
	var name string
	if e := a.Store.DB.QueryRow(c.Context(), "SELECT filename FROM files WHERE id=$1 AND deleted_at IS NULL", id).Scan(&name); e != nil {
		return httpx.Error(c, 404, "file not found")
	}
	if a.Cfg.StorageDriver == "local" {
		_ = os.Remove(filepath.Join(a.Cfg.StoragePath, filepath.Base(name)))
	}
	_, e := a.Store.DB.Exec(c.Context(), "UPDATE files SET deleted_at=now(),updated_at=now() WHERE id=$1", id)
	if e != nil {
		return a.fail(c, e)
	}
	return a.ok(c, map[string]any{"message": "File deleted successfully"})
}
func (a *API) fileCleanup(c *fiber.Ctx) error {
	r, e := a.Store.DB.Exec(c.Context(), "DELETE FROM files WHERE deleted_at < now()-interval '30 days'")
	if e != nil {
		return a.fail(c, e)
	}
	return a.ok(c, map[string]any{"deletedCount": r.RowsAffected(), "message": fmt.Sprintf("Successfully deleted %d orphaned files older than 30 days", r.RowsAffected())})
}
func (a *API) logs(c *fiber.Ctx) error {
	rows, e := a.Store.DB.Query(c.Context(), "SELECT id,level,message,context,created_at FROM app_logs ORDER BY created_at DESC LIMIT 100")
	if e != nil {
		return a.fail(c, e)
	}
	defer rows.Close()
	out := make([]map[string]any, 0)
	for rows.Next() {
		var id int64
		var level, message string
		var contextJSON []byte
		var at time.Time
		if rows.Scan(&id, &level, &message, &contextJSON, &at) == nil {
			var ctx any
			_ = json.Unmarshal(contextJSON, &ctx)
			out = append(out, map[string]any{"id": id, "level": level, "message": message, "context": ctx, "createdAt": at})
		}
	}
	return a.ok(c, out)
}
func (a *API) ws(c *websocket.Conn) {
	a.clientsMu.Lock()
	a.clients[c] = struct{}{}
	a.clientsMu.Unlock()
	defer func() { a.clientsMu.Lock(); delete(a.clients, c); a.clientsMu.Unlock() }()
	defer c.Close()
	for {
		if _, _, e := c.ReadMessage(); e != nil {
			return
		}
	}
}
