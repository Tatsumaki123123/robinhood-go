package api

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"robinhood-go/internal/chain"
)

const (
	nativeAddress = chain.NativeAddress
	wethAddress   = "0x0bd7d308f8e1639fab988df18a8011f41eacad73"
	usdgAddress   = "0x5fc5360d0400a0fd4f2af552add042d716f1d168"
)

type routeHop struct {
	PoolID      string
	Currency0   string
	Currency1   string
	Fee         int64
	TickSpacing int64
	Hooks       string
	TokenIn     string
	TokenOut    string
	HookData    string
}

func (h routeHop) mapValue() map[string]any {
	return map[string]any{
		"poolId": h.PoolID, "currency0": h.Currency0, "currency1": h.Currency1,
		"fee": h.Fee, "tickSpacing": h.TickSpacing, "hooks": h.Hooks,
		"tokenIn": h.TokenIn, "tokenOut": h.TokenOut, "hookData": h.HookData,
	}
}

func normalizePoolID(v string) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if v != "" && !strings.HasPrefix(v, "0x") {
		v = "0x" + v
	}
	return v
}

func normalizeAddress(v string) string { return strings.ToLower(strings.TrimSpace(v)) }

func aveTaxValue(item map[string]any, keys ...string) *string {
	for _, key := range keys {
		v, ok := item[key]
		if !ok || v == nil {
			continue
		}
		switch x := v.(type) {
		case string:
			if strings.TrimSpace(x) != "" {
				value := strings.TrimSpace(x)
				return &value
			}
		case float64:
			value := strconv.FormatFloat(x, 'f', -1, 64)
			return &value
		case json.Number:
			value := string(x)
			return &value
		}
	}
	return nil
}

func pairValue(pair map[string]any, keys ...string) any {
	for _, key := range keys {
		if v, ok := pair[key]; ok && v != nil {
			return v
		}
		for _, containerKey := range []string{"rawData", "data", "pool"} {
			if container, ok := pair[containerKey].(map[string]any); ok {
				if v, exists := container[key]; exists && v != nil {
					return v
				}
			}
		}
	}
	return nil
}

func pairAddress(pair map[string]any, keys ...string) string {
	for _, key := range keys {
		if v := pairValue(pair, key); v != nil {
			s := normalizeAddress(fmt.Sprint(v))
			if len(s) == 42 && strings.HasPrefix(s, "0x") {
				return s
			}
		}
	}
	return ""
}

func pairNumber(pair map[string]any, keys ...string) (int64, bool) {
	for _, key := range keys {
		v := pairValue(pair, key)
		if v == nil {
			continue
		}
		switch x := v.(type) {
		case float64:
			if x == float64(int64(x)) {
				return int64(x), true
			}
		case int:
			return int64(x), true
		case int64:
			return x, true
		case string:
			n, err := strconv.ParseInt(x, 10, 64)
			if err == nil {
				return n, true
			}
		}
	}
	return 0, false
}

func isV4Pair(pair map[string]any) bool {
	amm := strings.ToLower(fmt.Sprint(pairValue(pair, "amm")))
	return amm == "v4" || amm == "uniswapv4" || strings.Contains(amm, "uniswap_v4")
}

func pairMatches(pair map[string]any, left, right string) bool {
	a := pairAddress(pair, "token0_address", "token0Address", "currency0", "currency0_address")
	b := pairAddress(pair, "token1_address", "token1Address", "currency1", "currency1_address")
	left, right = normalizeAddress(left), normalizeAddress(right)
	return a != "" && b != "" && ((a == left && b == right) || (a == right && b == left))
}

func routeHopFromPair(pair map[string]any, tokenIn, tokenOut string) (routeHop, bool) {
	tokenIn, tokenOut = normalizeAddress(tokenIn), normalizeAddress(tokenOut)
	poolID := normalizePoolID(fmt.Sprint(pairValue(pair, "pair")))
	if poolID == "0x" || poolID == "" {
		poolID = normalizePoolID(fmt.Sprint(pairValue(pair, "poolId")))
	}
	if len(poolID) != 66 {
		return routeHop{}, false
	}
	currency0 := pairAddress(pair, "token0_address", "token0Address", "currency0", "currency0_address")
	currency1 := pairAddress(pair, "token1_address", "token1Address", "currency1", "currency1_address")
	fee, feeOK := pairNumber(pair, "fee", "pool_fee", "poolFee")
	tick, tickOK := pairNumber(pair, "tick_spacing", "tickSpacing")
	hooks := pairAddress(pair, "hooks", "hook", "hook_address")
	if currency0 == "" || currency1 == "" || !feeOK || !tickOK || hooks == "" {
		return routeHop{}, false
	}
	if currency0 > currency1 || !((currency0 == tokenIn || currency0 == tokenOut) && (currency1 == tokenIn || currency1 == tokenOut)) {
		return routeHop{}, false
	}
	calculated, err := chain.PoolID(map[string]any{"currency0": currency0, "currency1": currency1, "fee": fee, "tickSpacing": tick, "hooks": hooks})
	if err != nil || !strings.EqualFold(calculated, poolID) {
		return routeHop{}, false
	}
	return routeHop{PoolID: poolID, Currency0: currency0, Currency1: currency1, Fee: fee, TickSpacing: tick, Hooks: hooks, TokenIn: tokenIn, TokenOut: tokenOut, HookData: "0x"}, true
}

func inferredHop(poolID, tokenIn, tokenOut string, hooksList []string) (routeHop, bool) {
	tokenIn, tokenOut = normalizeAddress(tokenIn), normalizeAddress(tokenOut)
	c0, c1 := tokenIn, tokenOut
	if c1 < c0 {
		c0, c1 = c1, c0
	}
	for _, fee := range []int64{0, 100, 500, 3000, 10000} {
		for _, tick := range []int64{1, 10, 20, 60, 100, 200} {
			for _, hooks := range hooksList {
				calculated, err := chain.PoolID(map[string]any{"currency0": c0, "currency1": c1, "fee": fee, "tickSpacing": tick, "hooks": hooks})
				if err == nil && strings.EqualFold(calculated, poolID) {
					return routeHop{PoolID: normalizePoolID(poolID), Currency0: c0, Currency1: c1, Fee: fee, TickSpacing: tick, Hooks: normalizeAddress(hooks), TokenIn: tokenIn, TokenOut: tokenOut, HookData: "0x"}, true
				}
			}
		}
	}
	return routeHop{}, false
}

func (a *API) resolveAveHop(ctx context.Context, pair map[string]any, tokenIn, tokenOut string) (routeHop, bool) {
	if hop, ok := routeHopFromPair(pair, tokenIn, tokenOut); ok {
		return hop, true
	}
	poolID := normalizePoolID(fmt.Sprint(pairValue(pair, "pair")))
	if poolID == "0x" || poolID == "" {
		poolID = normalizePoolID(fmt.Sprint(pairValue(pair, "poolId")))
	}
	if hop, ok := inferredHop(poolID, tokenIn, tokenOut, []string{nativeAddress, a.Cfg.PonsExternalHooks}); ok {
		return hop, true
	}
	// Graduated Pons pools expose their fee and tick spacing through the
	// factory even when AVE omits the PoolKey fields.
	if a.RPC != nil {
		launched, err := a.RPC.LaunchedToken(ctx, a.Cfg.PonsFactory, tokenOut)
		if err == nil && launched.Exists && strings.EqualFold(launched.PairToken.Hex(), tokenIn) {
			c0, c1 := normalizeAddress(tokenIn), normalizeAddress(tokenOut)
			if c1 < c0 {
				c0, c1 = c1, c0
			}
			calculated, err := chain.PoolID(map[string]any{"currency0": c0, "currency1": c1, "fee": launched.PoolFee, "tickSpacing": launched.TickSpacing, "hooks": a.Cfg.PonsExternalHooks})
			if err == nil && strings.EqualFold(calculated, poolID) {
				return routeHop{PoolID: poolID, Currency0: c0, Currency1: c1, Fee: bigIntToInt64(launched.PoolFee), TickSpacing: bigIntToInt64(launched.TickSpacing), Hooks: normalizeAddress(a.Cfg.PonsExternalHooks), TokenIn: normalizeAddress(tokenIn), TokenOut: normalizeAddress(tokenOut), HookData: "0x"}, true
			}
		}
	}
	return routeHop{}, false
}

func fixedWethUSDGHop() routeHop {
	return routeHop{PoolID: "0xfcfae8fa0bd6da961bcf5d990f27690932deac4f093e99bf3e871691c6586593", Currency0: wethAddress, Currency1: usdgAddress, Fee: 500, TickSpacing: 10, Hooks: nativeAddress, TokenIn: wethAddress, TokenOut: usdgAddress, HookData: "0x"}
}

func (a *API) discoverNativeRoute(ctx context.Context, userID int64, targetToken, targetPool, quote string, target routeHop) ([]routeHop, bool) {
	targetToken, quote = normalizeAddress(targetToken), normalizeAddress(quote)
	if quote == nativeAddress || quote == wethAddress {
		return []routeHop{target}, true
	}
	if quote == usdgAddress {
		h := fixedWethUSDGHop()
		h.TokenIn, h.TokenOut = wethAddress, usdgAddress
		return []routeHop{h, target}, true
	}
	detail, err := a.aveJSON(ctx, "/v2api/token_info/v1/token/detail", map[string]string{"token_id": quote + "-robinhood", "cache_use": "false"})
	if err != nil {
		return nil, false
	}
	data, _ := detail["data"].(map[string]any)
	pairs, _ := data["pairs"].([]any)
	for _, raw := range pairs {
		pair, ok := raw.(map[string]any)
		if !ok || !isV4Pair(pair) {
			continue
		}
		if pairMatches(pair, quote, usdgAddress) {
			if hop, ok := a.resolveAveHop(ctx, pair, usdgAddress, quote); ok {
				return []routeHop{fixedWethUSDGHop(), hop, target}, true
			}
		}
	}
	for _, raw := range pairs {
		pair, ok := raw.(map[string]any)
		if !ok || !isV4Pair(pair) || !pairMatches(pair, quote, wethAddress) {
			continue
		}
		if hop, ok := a.resolveAveHop(ctx, pair, wethAddress, quote); ok {
			return []routeHop{hop, target}, true
		}
	}
	return nil, false
}

func reverseRoute(hops []routeHop) []routeHop {
	out := make([]routeHop, len(hops))
	for i := range hops {
		h := hops[len(hops)-1-i]
		h.TokenIn, h.TokenOut = h.TokenOut, h.TokenIn
		out[i] = h
	}
	return out
}

func (a *API) persistDiscoveredRoute(ctx context.Context, userID int64, targetToken, targetPool string, hops []routeHop) error {
	buy := make([]any, len(hops))
	for i, hop := range hops {
		buy[i] = hop.mapValue()
	}
	sellHops := reverseRoute(hops)
	sell := make([]any, len(sellHops))
	for i, hop := range sellHops {
		sell[i] = hop.mapValue()
	}
	if _, err := a.Store.SaveRoute(ctx, userID, targetPool, nativeAddress, targetToken, "buy", buy, targetToken); err != nil {
		return err
	}
	_, err := a.Store.SaveRoute(ctx, userID, targetPool, targetToken, nativeAddress, "sell", sell, targetToken)
	return err
}
