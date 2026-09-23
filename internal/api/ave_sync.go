package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
	"robinhood-go/internal/chain"
	"robinhood-go/internal/store"
	"robinhood-go/internal/strategy"
)

const (
	monitorTokenRefreshInterval = time.Hour
	aveWalletRefreshInterval    = time.Minute
)

// StartAveDataRefresh mirrors the Node service's slow background
// reconciliation jobs. AVE is intentionally kept off the trading event path.
func (a *API) StartAveDataRefresh(ctx context.Context) {
	refreshTokens := func() {
		if err := a.refreshMonitorTokenData(ctx); err != nil {
			if a.Log != nil {
				a.Log.Warn("定时刷新 monitorToken 失败", zap.Error(err))
			}
		}
	}
	refreshWallets := func() {
		if err := a.refreshAveWalletPositions(ctx); err != nil {
			if a.Log != nil {
				a.Log.Warn("定时同步 AVE 持仓失败", zap.Error(err))
			}
		}
	}
	go refreshTokens()
	go refreshWallets()
	go func() {
		ticker := time.NewTicker(monitorTokenRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refreshTokens()
			}
		}
	}()
	go func() {
		ticker := time.NewTicker(aveWalletRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refreshWallets()
			}
		}
	}()
}

func (a *API) refreshMonitorTokenData(ctx context.Context) error {
	a.aveRefreshMu.Lock()
	defer a.aveRefreshMu.Unlock()
	users, err := a.Store.Users(ctx)
	if err != nil {
		return err
	}
	var firstErr error
	for _, user := range users {
		if !user.Enabled || !strategy.Parse(user.Config).Enabled {
			continue
		}
		enabled := true
		tokens, tokenErr := a.Store.Tokens(ctx, user.UserID, &enabled)
		if tokenErr != nil {
			if firstErr == nil {
				firstErr = tokenErr
			}
			continue
		}
		for _, token := range tokens {
			if err := a.syncMonitorToken(ctx, token); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}
	a.invalidateCache()
	return firstErr
}

func (a *API) syncMonitorToken(ctx context.Context, token store.Token) error {
	if a.Cfg.AveBaseURL == "" {
		return fmt.Errorf("AVE service is unavailable")
	}
	detail, err := a.aveJSON(ctx, "/v2api/token_info/v1/token/detail", map[string]string{
		"token_id":  strings.ToLower(token.TokenAddress) + "-robinhood",
		"cache_use": "false",
	})
	if err != nil {
		return err
	}
	data, _ := detail["data"].(map[string]any)
	tokenDetail, _ := data["token"].(map[string]any)
	pairs, _ := data["pairs"].([]any)
	for _, raw := range pairs {
		pair, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		if !strings.EqualFold(pairAddress(pair, "token0_address", "token0Address"), token.TokenAddress) &&
			!strings.EqualFold(pairAddress(pair, "token1_address", "token1Address"), token.TokenAddress) {
			continue
		}
		if token.QuoteTokenAddress != "" && !pairMatches(pair, token.TokenAddress, token.QuoteTokenAddress) &&
			!strings.EqualFold(token.QuoteTokenAddress, chain.NativeAddress) {
			continue
		}
		sources := []map[string]any{pair, tokenDetail, data}
		if value := aveStringPtr(sources, "name", "token_name", "tokenName", "name_en", "name_zh"); value != nil {
			token.Name = value
		}
		if value := aveStringPtr(sources, "symbol", "token_symbol", "tokenSymbol"); value != nil {
			token.Symbol = value
		}
		if value := aveStringPtr(sources, "token_logo_url", "tokenLogoUrl", "logo_url", "logo"); value != nil {
			token.TokenLogoURL = value
		}
		targetIsToken0 := strings.EqualFold(str(pairValue(pair, "token0_address")), token.TokenAddress)
		poolID := normalizePoolID(str(pairValue(pair, "pair", "poolId")))
		targetPriceUSDKey, targetPriceEthKey := "token1_price_usd", "token1_price_eth"
		quotePriceUSDKey, quoteDecimalsKey := "token0_price_usd", "token0_decimal"
		if targetIsToken0 {
			targetPriceUSDKey, targetPriceEthKey = "token0_price_usd", "token0_price_eth"
			quotePriceUSDKey, quoteDecimalsKey = "token1_price_usd", "token1_decimal"
		}
		quoteAddress := pairAddress(pair, "token0_address", "token0Address")
		quoteSymbol := str(pairValue(pair, "token0_symbol"))
		quoteLogo := str(pairValue(pair, "token0_logo_url"))
		if targetIsToken0 {
			quoteAddress = pairAddress(pair, "token1_address", "token1Address")
			quoteSymbol = str(pairValue(pair, "token1_symbol"))
			quoteLogo = str(pairValue(pair, "token1_logo_url"))
		}
		quotePriceEthKey := "token0_price_eth"
		if targetIsToken0 {
			quotePriceEthKey = "token1_price_eth"
		}
		quotePriceEth := str(pairValue(pair, quotePriceEthKey))
		if strings.EqualFold(quoteAddress, chain.NativeAddress) {
			quotePriceEth = "1"
		}
		if poolID != "" && quoteAddress != "" {
			rawPair, _ := json.Marshal(pair)
			_, _ = a.Store.DB.Exec(ctx, `INSERT INTO ave_token_pools(token_address,pool_id,amm,quote_token_address,quote_token_symbol,quote_token_decimals,quote_price_eth,quote_price_usd,token_price_eth,token_price_usd,raw_data,observed_at) VALUES($1,$2,NULLIF($3,''),$4,NULLIF($5,''),NULLIF($6,0),NULLIF($7,''),NULLIF($8,''),NULLIF($9,''),NULLIF($10,''),$11::jsonb,now()) ON CONFLICT(token_address,pool_id) DO UPDATE SET amm=EXCLUDED.amm,quote_token_address=EXCLUDED.quote_token_address,quote_token_symbol=EXCLUDED.quote_token_symbol,quote_token_decimals=EXCLUDED.quote_token_decimals,quote_price_eth=EXCLUDED.quote_price_eth,quote_price_usd=EXCLUDED.quote_price_usd,token_price_eth=EXCLUDED.token_price_eth,token_price_usd=EXCLUDED.token_price_usd,raw_data=EXCLUDED.raw_data,observed_at=now(),updated_at=now()`, token.TokenAddress, poolID, strings.ToLower(str(pairValue(pair, "amm"))), quoteAddress, quoteSymbol, id(pairValue(pair, quoteDecimalsKey)), quotePriceEth, str(pairValue(pair, quotePriceUSDKey)), str(pairValue(pair, targetPriceEthKey)), str(pairValue(pair, targetPriceUSDKey)), string(rawPair))
			if quotePriceEth != "" {
				_, _ = a.Store.DB.Exec(ctx, `UPDATE monitor_routes SET target_quote_price_eth=$3,updated_at=now() WHERE target_token=$1 AND target_pool_id=$2`, strings.ToLower(token.TokenAddress), poolID, quotePriceEth)
			}
		}
		if poolID != "" {
			token.Pair = poolID
		}
		token.Amm = strings.ToLower(str(pairValue(pair, "amm")))
		if quoteSymbol != "" {
			token.QuoteTokenSymbol = &quoteSymbol
		}
		if quoteLogo != "" {
			token.QuoteTokenLogoURL = &quoteLogo
		}
		if value := pairValue(pair, "market_cap", "marketCap"); value != nil {
			s := str(value)
			token.MarketCap = &s
		}
		if value := pairValue(pair, targetPriceEthKey); value != nil {
			s := str(value)
			token.TokenPriceEth = &s
		}
		if value := pairValue(pair, targetPriceUSDKey); value != nil {
			s := str(value)
			token.TokenPriceUsd = &s
		}
		if value := pairValue(pair, quotePriceUSDKey); value != nil {
			s := str(value)
			token.QuoteUSDPrice = &s
		}
		if value := pairValue(pair, quoteDecimalsKey); value != nil {
			n := int(id(value))
			token.QuoteDecimals = &n
		}
		if value := pairValue(pair, "holders", "holders_count"); value != nil {
			n := id(value)
			token.HoldersCount = &n
		}
		if value := pairValue(pair, "buy_count_5m", "buys_tx_5m_count"); value != nil {
			n := id(value)
			token.BuyCount5m = &n
		}
		if value := pairValue(pair, "sell_count_5m", "sells_tx_5m_count"); value != nil {
			n := id(value)
			token.SellCount5m = &n
		}
		if value := pairValue(pair, "volume_5m"); value != nil {
			s := str(value)
			token.Volume5m = &s
		}
		if value := pairValue(pair, "price_change_5m"); value != nil {
			s := str(value)
			token.PriceChange5m = &s
		}
		if value := pairValue(pair, "price_change_1h"); value != nil {
			s := str(value)
			token.PriceChange1h = &s
		}
		if value := pairValue(pair, "price_change_24h"); value != nil {
			s := str(value)
			token.PriceChange24h = &s
		}
		if value := pairValue(pair, "buy_tax", "buyTax"); value != nil {
			s := str(value)
			token.BuyTax = &s
		}
		if value := pairValue(pair, "sell_tax", "sellTax"); value != nil {
			s := str(value)
			token.SellTax = &s
		}
		if (token.MarketCap == nil || strings.TrimSpace(*token.MarketCap) == "") && token.TotalSupplyRaw != nil && token.Decimals != nil && token.TokenPriceUsd != nil {
			token.MarketCap = derivedMarketCapFromSupply(*token.TotalSupplyRaw, *token.Decimals, *token.TokenPriceUsd)
		}
		_, err = a.Store.UpsertToken(ctx, token)
		return err
	}
	return nil
}

func (a *API) refreshAveWalletPositions(ctx context.Context) error {
	a.aveRefreshMu.Lock()
	defer a.aveRefreshMu.Unlock()
	if a.Cfg.AveBaseURL == "" {
		return fmt.Errorf("AVE service is unavailable")
	}
	users, err := a.Store.Users(ctx)
	if err != nil {
		return err
	}
	var firstErr error
	for _, user := range users {
		if !user.Enabled || strings.TrimSpace(user.WalletAddress) == "" {
			continue
		}
		walletTokens, walletErr := a.aveWalletTokens(ctx, user.WalletAddress)
		if walletErr != nil {
			if firstErr == nil {
				firstErr = walletErr
			}
			continue
		}
		tokens, tokenErr := a.Store.Tokens(ctx, user.UserID, nil)
		if tokenErr != nil {
			if firstErr == nil {
				firstErr = tokenErr
			}
			continue
		}
		byAddress := make(map[string]map[string]any, len(walletTokens))
		for _, item := range walletTokens {
			address := strings.ToLower(strings.TrimSpace(str(firstAny(item, "token", "token_address", "tokenAddress", "address", "contract_address"))))
			address = strings.TrimSuffix(address, "-robinhood")
			if address != "" && !strings.HasPrefix(address, "0x") {
				address = "0x" + address
			}
			if address != "" {
				byAddress[address] = item
			}
		}
		for _, token := range tokens {
			if item, ok := byAddress[strings.ToLower(token.TokenAddress)]; ok {
				if syncErr := a.syncAveWalletToken(ctx, token, item); syncErr != nil && firstErr == nil {
					firstErr = syncErr
				}
			}
		}
	}
	return firstErr
}

func (a *API) syncAveWalletToken(ctx context.Context, token store.Token, item map[string]any) error {
	decimals := 18
	if token.Decimals != nil && *token.Decimals >= 0 {
		decimals = *token.Decimals
	}
	balanceRaw, ok := aveDecimalToRaw(firstAny(item, "balance_amount", "balanceAmount", "balance"), decimals)
	if !ok {
		return nil
	}
	balanceInt, balanceOK := new(big.Int).SetString(balanceRaw, 10)
	if !balanceOK || balanceInt.Sign() < 0 {
		return nil
	}
	minimumBalanceRaw := new(big.Int).Mul(big.NewInt(100), new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil))
	belowMinimum := balanceInt.Cmp(minimumBalanceRaw) < 0
	averageUSD := strings.TrimSpace(str(firstAny(item, "average_net_purchase_price", "averageNetPurchasePrice")))
	averageCostRaw, averageOK := aveDecimalToRaw(averageUSD, 8)
	if averageCostRaw == "0" {
		averageOK = false
	}
	tx, err := a.Store.DB.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	var oldAmount, oldQuote, oldCost, oldAverage string
	readErr := tx.QueryRow(ctx, `SELECT token_amount_raw::text,quote_spent_raw,cost_usd_raw,average_cost_usd::text FROM strategy_positions WHERE user_id=$1 AND token_address=$2 AND curve_address=$3 FOR UPDATE`, token.UserID, token.TokenAddress, token.CurveAddress).Scan(&oldAmount, &oldQuote, &oldCost, &oldAverage)
	if errors.Is(readErr, pgx.ErrNoRows) {
		if balanceRaw == "0" || !averageOK {
			return tx.Commit(ctx)
		}
		oldAmount, oldQuote, oldCost, oldAverage = "0", "0", "0", "0"
	} else if readErr != nil {
		return readErr
	}
	if balanceRaw == "0" || belowMinimum {
		if _, err = tx.Exec(ctx, `UPDATE strategy_positions SET token_amount_raw='0',quote_spent_raw='0',cost_usd_raw='0',average_cost_usd=0,first_buy_price=0,last_buy_price=0,buy_count=0,sell_count=0,profit_sell_level=0,first_bought_at=NULL,next_scheduled_sell_at=NULL,external_cooldown_until=NULL,last_buy_at=NULL,updated_at=now() WHERE user_id=$1 AND token_address=$2 AND curve_address=$3`, token.UserID, token.TokenAddress, token.CurveAddress); err != nil {
			return err
		}
		_, _ = tx.Exec(ctx, `UPDATE monitor_records SET remain_token_amount_raw='0',remain_quote_amount_raw='0',remain_cost_usd_raw='0',average_cost_usd_raw='0' WHERE id=(SELECT id FROM monitor_records WHERE user_id=$1 AND token_address=$2 AND curve_address=$3 ORDER BY created_at DESC,id DESC LIMIT 1)`, token.UserID, token.TokenAddress, token.CurveAddress)
		return tx.Commit(ctx)
	}
	quoteRaw := oldQuote
	if oldAmount != "" && oldAmount != "0" {
		oldAmountInt, oldAmountOK := new(big.Int).SetString(oldAmount, 10)
		oldQuoteInt, oldQuoteOK := new(big.Int).SetString(strings.TrimSpace(oldQuote), 10)
		newAmountInt, newAmountOK := new(big.Int).SetString(balanceRaw, 10)
		if oldAmountOK && oldQuoteOK && newAmountOK && oldAmountInt.Sign() > 0 {
			quoteRaw = new(big.Int).Div(new(big.Int).Mul(oldQuoteInt, newAmountInt), oldAmountInt).String()
		}
	}
	if !averageOK {
		averageCostRaw = oldCost
		averageUSD = oldAverage
	}
	if averageCostRaw == "" {
		averageCostRaw = "0"
	}
	if averageUSD == "" {
		averageUSD = "0"
	}
	_, err = tx.Exec(ctx, `INSERT INTO strategy_positions(user_id,token_address,curve_address,token_amount_raw,quote_spent_raw,cost_usd_raw,average_cost_usd,buy_count,sell_count,profit_sell_level,updated_at) VALUES($1,$2,$3,$4,$5,$6,$7,0,0,0,now()) ON CONFLICT(user_id,token_address,curve_address) DO UPDATE SET token_amount_raw=EXCLUDED.token_amount_raw,quote_spent_raw=EXCLUDED.quote_spent_raw,cost_usd_raw=EXCLUDED.cost_usd_raw,average_cost_usd=EXCLUDED.average_cost_usd,updated_at=now()`, token.UserID, token.TokenAddress, token.CurveAddress, balanceRaw, quoteRaw, averageCostRaw, averageUSD)
	if err != nil {
		return err
	}
	_, _ = tx.Exec(ctx, `UPDATE monitor_records SET remain_token_amount_raw=$4,remain_quote_amount_raw=$5,remain_cost_usd_raw=$6,average_cost_usd_raw=$7 WHERE id=(SELECT id FROM monitor_records WHERE user_id=$1 AND token_address=$2 AND curve_address=$3 ORDER BY created_at DESC,id DESC LIMIT 1)`, token.UserID, token.TokenAddress, token.CurveAddress, balanceRaw, quoteRaw, averageCostRaw, averageUSD)
	return tx.Commit(ctx)
}

func aveDecimalToRaw(value any, decimals int) (string, bool) {
	if decimals < 0 || decimals > 255 {
		return "", false
	}
	rat, ok := decimalRat(value)
	if !ok || rat.Sign() < 0 {
		return "", false
	}
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(decimals)), nil)
	scaled := new(big.Rat).Mul(rat, new(big.Rat).SetInt(scale))
	return new(big.Int).Quo(scaled.Num(), scaled.Denom()).String(), true
}
