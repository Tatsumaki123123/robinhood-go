package strategy

import (
	"encoding/json"
	"sort"
	"strings"
)

type TokenRule struct {
	MaxMCP                 float64 `json:"maxMcp"`
	BuyUSD                 float64 `json:"buyUSD"`
	BuyRatio               float64 `json:"buyRatio"`
	MinSellRatio           float64 `json:"minSellRatio"`
	MinBuyUSD              float64 `json:"minBuyUSD"`
	MinBuyRatio            float64 `json:"minBuyRatio"`
	OnceSellRatio          float64 `json:"onceSellRatio"`
	OnceSellBaseUSD        float64 `json:"onceSellBaseUSD"`
	OnceSellBuyImpactRatio float64 `json:"onceSellBuyImpactRatio"`
}
type SellPolicy struct {
	FullSellAfterSellCount int  `json:"fullSellAfterSellCount"`
	ResetSellCountOnBuy    bool `json:"resetSellCountOnBuy"`
}
type ScheduledSell struct {
	Enabled        bool    `json:"enabled"`
	IntervalSecond int     `json:"intervalSecond"`
	SellRatio      float64 `json:"sellRatio"`
	BaseUSD        float64 `json:"baseUSD"`
	ResetOnBuy     bool    `json:"resetOnBuy"`
}
type ExternalBuySell struct {
	Enabled        bool    `json:"enabled"`
	MinBuyUSD      float64 `json:"minBuyUSD"`
	BuyImpactRatio float64 `json:"buyImpactRatio"`
	NeedProfit     bool    `json:"needProfit"`
	ProfitRatio    float64 `json:"profitRatio"`
	SellRatio      float64 `json:"sellRatio"`
	BuyAmountRatio float64 `json:"buyAmountRatio"`
	CooldownSecond int     `json:"cooldownSecond"`
}
type ProfitLevel struct {
	ProfitRatio float64 `json:"profitRatio"`
	SellRatio   float64 `json:"sellRatio"`
}
type ProfitSell struct {
	Enabled    bool          `json:"enabled"`
	Levels     []ProfitLevel `json:"levels"`
	ResetOnBuy bool          `json:"resetOnBuy"`
}
type LossSell struct {
	Enabled      bool    `json:"enabled"`
	TriggerRatio float64 `json:"triggerRatio"`
	SellAll      bool    `json:"sellAll"`
}
type Config struct {
	Name                    string          `json:"name"`
	Enabled                 bool            `json:"enabled"`
	MinMCP                  float64         `json:"minMcp"`
	Slippage                float64         `json:"slippage"`
	DiffBuySecond           int             `json:"diffBuySecond"`
	AutoSellSecond          int             `json:"autoSellSecond"`
	AutoProfitRatio         float64         `json:"autoProfitRatio"`
	AutoLossRatio           float64         `json:"autoLossRatio"`
	MaxLossBuyTimes         int             `json:"maxLossBuyTimes"`
	MinLossBuyRatio         float64         `json:"minLossBuyRatio"`
	ReplacePending          bool            `json:"replacePending"`
	PendingBuyTimeoutSecond int             `json:"pendingBuyTimeoutSecond"`
	TokenConfig             []TokenRule     `json:"tokenConfig"`
	SellPolicy              SellPolicy      `json:"sellPolicy"`
	ScheduledSell           ScheduledSell   `json:"scheduledSell"`
	ExternalBuySell         ExternalBuySell `json:"externalBuySell"`
	ProfitSell              ProfitSell      `json:"profitSell"`
	LossSell                LossSell        `json:"lossSell"`
	ListConfig              ListConfig      `json:"listConfig"`
}

type ListConfig struct {
	Source      string  `json:"source"`
	Category    string  `json:"category"`
	MinMCP      float64 `json:"minMcp"`
	MaxMCP      float64 `json:"maxMcp"`
	CreateDay   int     `json:"createDay"`
	From        string  `json:"from"`
	UserAddress string  `json:"userAddress"`
}

func Parse(v map[string]any) Config {
	if v == nil {
		v = map[string]any{}
	}
	// Start with the scalar defaults used by bottom-fishing.defaults.ts.  A
	// nested object's numeric fields still receive their Node fallback values,
	// while its enabled switch follows normalizeConfig's strict `=== true`
	// behavior (an omitted nested switch is disabled).
	c := Config{
		Name: "strategy", Enabled: true, MinMCP: 100000, Slippage: 0.2,
		ReplacePending: true, PendingBuyTimeoutSecond: 2,
		SellPolicy:      SellPolicy{FullSellAfterSellCount: 3, ResetSellCountOnBuy: true},
		ScheduledSell:   ScheduledSell{Enabled: true, IntervalSecond: 1250, SellRatio: 0.2, BaseUSD: 150, ResetOnBuy: false},
		ExternalBuySell: ExternalBuySell{Enabled: true, MinBuyUSD: 100, BuyImpactRatio: 0.03, NeedProfit: true, ProfitRatio: 0.05, SellRatio: 0.2, BuyAmountRatio: 0, CooldownSecond: 30},
		ProfitSell:      ProfitSell{Enabled: true, ResetOnBuy: true, Levels: []ProfitLevel{{0.05, 0.2}, {0.1, 0.25}, {0.2, 0.3}, {0.3, 1}}},
		LossSell:        LossSell{Enabled: true, TriggerRatio: 0.15, SellAll: true},
		// Node's normalizeConfig keeps nested strategy switches disabled when
		// the nested object is omitted.  The top-level default strategy is
		// enabled, while callers that want the optional sell paths must provide
		// their nested object (or use the full default config from the API).
		ListConfig: ListConfig{Source: "ave", Category: "pons_out_hot", MinMCP: 20000, MaxMCP: 200000, CreateDay: 1, From: "list"},
	}
	b, _ := json.Marshal(v)
	_ = json.Unmarshal(b, &c)
	// Node accepts both the public buyUSD spelling and its internal buyUsd
	// spelling during partial updates. Normalize the latter before using the
	// typed config so persisted legacy rows remain executable.
	if rules, ok := v["tokenConfig"].([]any); ok {
		for _, raw := range rules {
			if rule, ok := raw.(map[string]any); ok {
				if _, hasPublic := rule["buyUSD"]; !hasPublic {
					if internal, hasInternal := rule["buyUsd"]; hasInternal {
						rule["buyUSD"] = internal
					}
				}
			}
		}
		b, _ = json.Marshal(v)
		_ = json.Unmarshal(b, &c)
	}
	// json.Unmarshal cannot distinguish an omitted bool/number from an
	// explicitly supplied false/zero, so repair only fields absent from the
	// corresponding object. Explicit zero values therefore remain supported.
	if x, ok := v["sellPolicy"].(map[string]any); ok {
		if _, exists := x["fullSellAfterSellCount"]; !exists {
			c.SellPolicy.FullSellAfterSellCount = 3
		}
		if _, exists := x["resetSellCountOnBuy"]; !exists {
			c.SellPolicy.ResetSellCountOnBuy = true
		}
	}
	if x, ok := v["scheduledSell"].(map[string]any); ok {
		if _, exists := x["enabled"]; !exists {
			c.ScheduledSell.Enabled = false
		}
		if _, exists := x["intervalSecond"]; !exists {
			c.ScheduledSell.IntervalSecond = 1250
		}
		if _, exists := x["sellRatio"]; !exists {
			c.ScheduledSell.SellRatio = 0.2
		}
		if _, exists := x["baseUSD"]; !exists {
			c.ScheduledSell.BaseUSD = 150
		}
	} else {
		c.ScheduledSell = ScheduledSell{Enabled: false, IntervalSecond: 1250, SellRatio: 0.2, BaseUSD: 150}
	}
	if x, ok := v["externalBuySell"].(map[string]any); ok {
		if _, exists := x["enabled"]; !exists {
			c.ExternalBuySell.Enabled = false
		}
		if _, exists := x["minBuyUSD"]; !exists {
			c.ExternalBuySell.MinBuyUSD = 100
		}
		if _, exists := x["buyImpactRatio"]; !exists {
			c.ExternalBuySell.BuyImpactRatio = 0.03
		}
		if _, exists := x["needProfit"]; !exists {
			c.ExternalBuySell.NeedProfit = true
		}
		if _, exists := x["profitRatio"]; !exists {
			c.ExternalBuySell.ProfitRatio = 0.05
		}
		if _, exists := x["sellRatio"]; !exists {
			c.ExternalBuySell.SellRatio = 0.2
		}
		if _, exists := x["cooldownSecond"]; !exists {
			c.ExternalBuySell.CooldownSecond = 30
		}
	} else {
		c.ExternalBuySell = ExternalBuySell{Enabled: false, MinBuyUSD: 100, BuyImpactRatio: 0.03, NeedProfit: true, ProfitRatio: 0.05, SellRatio: 0.2, CooldownSecond: 30}
	}
	if x, ok := v["profitSell"].(map[string]any); ok {
		if _, exists := x["enabled"]; !exists {
			c.ProfitSell.Enabled = false
		}
		if _, exists := x["resetOnBuy"]; !exists {
			c.ProfitSell.ResetOnBuy = true
		}
		if _, exists := x["levels"]; !exists {
			c.ProfitSell.Levels = []ProfitLevel{{0.05, 0.2}, {0.1, 0.25}, {0.2, 0.3}, {0.3, 1}}
		}
	} else {
		c.ProfitSell = ProfitSell{Enabled: false, ResetOnBuy: true, Levels: []ProfitLevel{{0.05, 0.2}, {0.1, 0.25}, {0.2, 0.3}, {0.3, 1}}}
	}
	if x, ok := v["lossSell"].(map[string]any); ok {
		if _, exists := x["enabled"]; !exists {
			c.LossSell.Enabled = false
		}
		if _, exists := x["triggerRatio"]; !exists {
			c.LossSell.TriggerRatio = 0.15
		}
		if _, exists := x["sellAll"]; !exists {
			c.LossSell.SellAll = true
		}
	} else {
		c.LossSell = LossSell{Enabled: false, TriggerRatio: 0.15, SellAll: true}
	}
	if _, ok := v["enabled"]; !ok {
		c.Enabled = true
	}
	if _, ok := v["minMcp"]; !ok {
		c.MinMCP = 100000
	}
	if _, ok := v["slippage"]; !ok {
		c.Slippage = 0.2
	}
	if _, ok := v["replacePending"]; !ok {
		c.ReplacePending = true
	}
	if raw, ok := v["tokenConfig"]; !ok || raw == nil {
		c.TokenConfig = []TokenRule{{MaxMCP: 200000, BuyUSD: 10, MinSellRatio: 0.01}}
	}
	if x, ok := v["listConfig"].(map[string]any); ok {
		if _, exists := x["source"]; !exists {
			c.ListConfig.Source = "ave"
		}
		if _, exists := x["category"]; !exists {
			c.ListConfig.Category = "pons_out_hot"
		}
		if _, exists := x["minMcp"]; !exists {
			c.ListConfig.MinMCP = 20000
		}
		if _, exists := x["maxMcp"]; !exists {
			c.ListConfig.MaxMCP = 200000
		}
		if _, exists := x["createDay"]; !exists {
			c.ListConfig.CreateDay = 1
		}
		if _, exists := x["from"]; !exists {
			c.ListConfig.From = "list"
		}
	} else if _, exists := v["listConfig"]; !exists {
		c.ListConfig = ListConfig{Source: "ave", Category: "pons_out_hot", MinMCP: 20000, MaxMCP: 200000, CreateDay: 1, From: "list"}
	}
	if c.MinMCP < 0 {
		c.MinMCP = 100000
	}
	if c.Slippage < 0 {
		c.Slippage = 0.2
	}
	if c.DiffBuySecond < 0 {
		c.DiffBuySecond = 0
	}
	if c.MaxLossBuyTimes < 0 {
		c.MaxLossBuyTimes = 0
	}
	if c.MinLossBuyRatio < 0 {
		c.MinLossBuyRatio = 0
	}
	if c.SellPolicy.FullSellAfterSellCount < 0 {
		c.SellPolicy.FullSellAfterSellCount = 3
	}
	if c.ScheduledSell.IntervalSecond < 0 {
		c.ScheduledSell.IntervalSecond = 1250
	}
	if c.ScheduledSell.SellRatio < 0 {
		c.ScheduledSell.SellRatio = 0.2
	}
	if c.ScheduledSell.BaseUSD < 0 {
		c.ScheduledSell.BaseUSD = 150
	}
	if c.ExternalBuySell.MinBuyUSD < 0 {
		c.ExternalBuySell.MinBuyUSD = 100
	}
	if c.ExternalBuySell.BuyImpactRatio < 0 {
		c.ExternalBuySell.BuyImpactRatio = 0.03
	}
	if c.ExternalBuySell.ProfitRatio < 0 {
		c.ExternalBuySell.ProfitRatio = 0.05
	}
	if c.ExternalBuySell.SellRatio < 0 {
		c.ExternalBuySell.SellRatio = 0.2
	}
	if c.ExternalBuySell.BuyAmountRatio < 0 {
		c.ExternalBuySell.BuyAmountRatio = 0
	}
	if c.ExternalBuySell.CooldownSecond < 0 {
		c.ExternalBuySell.CooldownSecond = 30
	}
	if c.LossSell.TriggerRatio < 0 {
		c.LossSell.TriggerRatio = 0.15
	}
	for i := range c.TokenConfig {
		if c.TokenConfig[i].MaxMCP < 0 {
			c.TokenConfig[i].MaxMCP = 0
		}
		if c.TokenConfig[i].BuyUSD < 0 {
			c.TokenConfig[i].BuyUSD = 0
		}
		if c.TokenConfig[i].BuyRatio < 0 {
			c.TokenConfig[i].BuyRatio = 0
		}
		if c.TokenConfig[i].MinSellRatio < 0 {
			c.TokenConfig[i].MinSellRatio = 0
		}
	}
	levels := c.ProfitSell.Levels[:0]
	for _, level := range c.ProfitSell.Levels {
		if level.ProfitRatio > 0 && level.SellRatio > 0 {
			levels = append(levels, level)
		}
	}
	c.ProfitSell.Levels = levels
	sort.SliceStable(c.TokenConfig, func(i, j int) bool { return c.TokenConfig[i].MaxMCP < c.TokenConfig[j].MaxMCP })
	sort.SliceStable(c.ProfitSell.Levels, func(i, j int) bool { return c.ProfitSell.Levels[i].ProfitRatio < c.ProfitSell.Levels[j].ProfitRatio })
	c.Name = strings.TrimSpace(c.Name)
	if c.Name == "" {
		c.Name = "strategy"
	}
	return c
}
func (c Config) Rule(mcp float64) (TokenRule, bool) {
	r, _, ok := c.RuleWithIndex(mcp)
	return r, ok
}

// RuleWithIndex returns the first token rule that matches the market cap. The
// index is useful for diagnostics because several rules can be configured for
// one strategy.
func (c Config) RuleWithIndex(mcp float64) (TokenRule, int, bool) {
	for i, r := range c.TokenConfig {
		if r.MaxMCP == 0 || mcp <= r.MaxMCP {
			return r, i, true
		}
	}
	return TokenRule{}, -1, false
}
