package store

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
	"strings"
	"time"
)

type Store struct{ DB *pgxpool.Pool }

func New(ctx context.Context, url string) (*Store, error) {
	db, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, err
	}
	if err = db.Ping(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return &Store{db}, nil
}
func (s *Store) Close() { s.DB.Close() }
func (s *Store) Migrate(ctx context.Context) error {
	b, err := os.ReadFile("migrations/001_init.sql")
	if err != nil {
		return err
	}
	_, err = s.DB.Exec(ctx, string(b))
	return err
}

type User struct {
	ID                  int64          `json:"id"`
	UserID              int64          `json:"userId"`
	WalletAddress       string         `json:"walletAddress"`
	Config              map[string]any `json:"config"`
	Enabled             bool           `json:"enabled"`
	CreatedAt           time.Time      `json:"createdAt"`
	UpdatedAt           time.Time      `json:"updatedAt"`
	PrivateKeyEncrypted *string        `json:"-"`
}

func scanUser(row interface{ Scan(...any) error }) (User, error) {
	var u User
	var cfg []byte
	err := row.Scan(&u.ID, &u.UserID, &u.WalletAddress, &u.PrivateKeyEncrypted, &cfg, &u.Enabled, &u.CreatedAt, &u.UpdatedAt)
	if err == nil {
		_ = json.Unmarshal(cfg, &u.Config)
	}
	return u, err
}
func (s *Store) Users(ctx context.Context) ([]User, error) {
	rows, err := s.DB.Query(ctx, "SELECT id,user_id,wallet_address,private_key_encrypted,config,enabled,created_at,updated_at FROM monitor_users ORDER BY user_id")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []User
	for rows.Next() {
		u, e := scanUser(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, u)
	}
	return out, rows.Err()
}
func (s *Store) User(ctx context.Context, id int64) (User, error) {
	return scanUser(s.DB.QueryRow(ctx, "SELECT id,user_id,wallet_address,private_key_encrypted,config,enabled,created_at,updated_at FROM monitor_users WHERE user_id=$1", id))
}
func (s *Store) UpsertUser(ctx context.Context, id int64, name, wallet, encrypted string, cfg map[string]any, enabled bool) (User, error) {
	if name == "" {
		name = fmt.Sprintf("User%d", id)
	}
	cfg["name"] = name
	b, _ := json.Marshal(cfg)
	var u User
	var raw []byte
	err := s.DB.QueryRow(ctx, `INSERT INTO monitor_users(user_id,wallet_address,private_key_encrypted,config,enabled) VALUES($1,$2,NULLIF($3,''),$4,$5) ON CONFLICT(user_id) DO UPDATE SET wallet_address=EXCLUDED.wallet_address,config=EXCLUDED.config,enabled=EXCLUDED.enabled,updated_at=now() RETURNING id,user_id,wallet_address,private_key_encrypted,config,enabled,created_at,updated_at`, id, wallet, encrypted, b, enabled).Scan(&u.ID, &u.UserID, &u.WalletAddress, &u.PrivateKeyEncrypted, &raw, &u.Enabled, &u.CreatedAt, &u.UpdatedAt)
	if err == nil {
		_ = json.Unmarshal(raw, &u.Config)
	}
	return u, err
}
func (s *Store) NextUserID(ctx context.Context) (int64, error) {
	var id int64
	err := s.DB.QueryRow(ctx, "SELECT COALESCE(MAX(user_id),0)+1 FROM monitor_users").Scan(&id)
	return id, err
}
func (s *Store) DeleteUser(ctx context.Context, id int64) error {
	_, e := s.DB.Exec(ctx, "DELETE FROM monitor_users WHERE user_id=$1", id)
	return e
}

type Token struct {
	ID                                                                                                                                                                  int64 `json:"id"`
	UserID                                                                                                                                                              int64 `json:"userId"`
	TokenAddress, CurveAddress, PoolID, Pair, Amm, Currency0, Currency1, Hooks, QuoteTokenAddress                                                                       string
	Fee, TickSpacing                                                                                                                                                    int
	Name, Symbol, TokenLogoURL, QuoteTokenSymbol, QuoteTokenLogoURL                                                                                                     *string
	Decimals                                                                                                                                                            *int
	TotalSupplyRaw, MarketCap, TokenPriceEth, TokenPriceUsd, PriceChange5m, PriceChange1h, PriceChange24h, Volume5m, QuoteUSDPrice, BuyTax, SellTax, LastProcessedBlock *string
	HoldersCount, BuyCount5m, SellCount5m                                                                                                                               *int64
	QuoteDecimals                                                                                                                                                       *int
	Enabled                                                                                                                                                             bool
	CreatedAt, UpdatedAt                                                                                                                                                time.Time
}

func (t Token) Map() map[string]any {
	return map[string]any{"id": t.ID, "userId": t.UserID, "tokenAddress": t.TokenAddress, "curveAddress": t.CurveAddress, "poolId": t.PoolID, "pair": t.Pair, "amm": t.Amm, "currency0": t.Currency0, "currency1": t.Currency1, "fee": t.Fee, "tickSpacing": t.TickSpacing, "hooks": t.Hooks, "quoteTokenAddress": t.QuoteTokenAddress, "name": t.Name, "symbol": t.Symbol, "tokenLogoUrl": t.TokenLogoURL, "quoteTokenSymbol": t.QuoteTokenSymbol, "quoteTokenLogoUrl": t.QuoteTokenLogoURL, "decimals": t.Decimals, "totalSupplyRaw": t.TotalSupplyRaw, "marketCap": t.MarketCap, "tokenPriceEth": t.TokenPriceEth, "tokenPriceUsd": t.TokenPriceUsd, "priceChange5m": t.PriceChange5m, "priceChange1h": t.PriceChange1h, "priceChange24h": t.PriceChange24h, "holdersCount": t.HoldersCount, "buyCount5m": t.BuyCount5m, "sellCount5m": t.SellCount5m, "volume5m": t.Volume5m, "quoteDecimals": t.QuoteDecimals, "quoteUsdPrice": t.QuoteUSDPrice, "buyTax": t.BuyTax, "sellTax": t.SellTax, "lastProcessedBlock": t.LastProcessedBlock, "enabled": t.Enabled, "createdAt": t.CreatedAt, "updatedAt": t.UpdatedAt}
}
func tokenArgs(t Token) []any {
	return []any{t.UserID, t.TokenAddress, t.CurveAddress, t.PoolID, t.Pair, t.Amm, t.Currency0, t.Currency1, t.Fee, t.TickSpacing, t.Hooks, t.QuoteTokenAddress, t.Name, t.Symbol, t.TokenLogoURL, t.QuoteTokenSymbol, t.QuoteTokenLogoURL, t.Decimals, t.TotalSupplyRaw, t.MarketCap, t.TokenPriceEth, t.TokenPriceUsd, t.PriceChange5m, t.PriceChange1h, t.PriceChange24h, t.HoldersCount, t.BuyCount5m, t.SellCount5m, t.Volume5m, t.QuoteDecimals, t.QuoteUSDPrice, t.BuyTax, t.SellTax, t.Enabled}
}

const tokenSelect = "id,user_id,token_address,curve_address,pool_id,pair,amm,currency0,currency1,fee,tick_spacing,hooks,quote_token_address,name,symbol,token_logo_url,quote_token_symbol,quote_token_logo_url,decimals,total_supply_raw,market_cap,token_price_eth,token_price_usd,price_change_5m,price_change_1h,price_change_24h,holders_count,buy_count_5m,sell_count_5m,volume_5m,quote_decimals,quote_usd_price,buy_tax,sell_tax,last_processed_block,enabled,created_at,updated_at"

func scanToken(row interface{ Scan(...any) error }) (Token, error) {
	var t Token
	err := row.Scan(&t.ID, &t.UserID, &t.TokenAddress, &t.CurveAddress, &t.PoolID, &t.Pair, &t.Amm, &t.Currency0, &t.Currency1, &t.Fee, &t.TickSpacing, &t.Hooks, &t.QuoteTokenAddress, &t.Name, &t.Symbol, &t.TokenLogoURL, &t.QuoteTokenSymbol, &t.QuoteTokenLogoURL, &t.Decimals, &t.TotalSupplyRaw, &t.MarketCap, &t.TokenPriceEth, &t.TokenPriceUsd, &t.PriceChange5m, &t.PriceChange1h, &t.PriceChange24h, &t.HoldersCount, &t.BuyCount5m, &t.SellCount5m, &t.Volume5m, &t.QuoteDecimals, &t.QuoteUSDPrice, &t.BuyTax, &t.SellTax, &t.LastProcessedBlock, &t.Enabled, &t.CreatedAt, &t.UpdatedAt)
	return t, err
}
func (s *Store) Tokens(ctx context.Context, user int64, enabled *bool) ([]Token, error) {
	q := "SELECT " + tokenSelect + " FROM monitor_tokens WHERE user_id=$1"
	args := []any{user}
	if enabled != nil {
		q += " AND enabled=$2"
		args = append(args, *enabled)
	}
	q += " ORDER BY id"
	rows, e := s.DB.Query(ctx, q, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []Token
	for rows.Next() {
		t, e := scanToken(rows)
		if e != nil {
			return nil, e
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) TokenEnabled(ctx context.Context, user int64, address, curve string) (bool, error) {
	var enabled bool
	err := s.DB.QueryRow(ctx, "SELECT enabled FROM monitor_tokens WHERE user_id=$1 AND lower(token_address)=lower($2) AND lower(curve_address)=lower($3)", user, address, curve).Scan(&enabled)
	return enabled, err
}

func (s *Store) Token(ctx context.Context, user int64, address, curve string) (Token, error) {
	return scanToken(s.DB.QueryRow(ctx, "SELECT "+tokenSelect+" FROM monitor_tokens WHERE user_id=$1 AND lower(token_address)=lower($2) AND lower(curve_address)=lower($3)", user, address, curve))
}
func (s *Store) UpsertToken(ctx context.Context, t Token) (Token, error) {
	q := `INSERT INTO monitor_tokens(user_id,token_address,curve_address,pool_id,pair,amm,currency0,currency1,fee,tick_spacing,hooks,quote_token_address,name,symbol,token_logo_url,quote_token_symbol,quote_token_logo_url,decimals,total_supply_raw,market_cap,token_price_eth,token_price_usd,price_change_5m,price_change_1h,price_change_24h,holders_count,buy_count_5m,sell_count_5m,volume_5m,quote_decimals,quote_usd_price,buy_tax,sell_tax,enabled)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,$29,$30,$31,$32,$33,$34)
ON CONFLICT(user_id,token_address,curve_address) DO UPDATE SET pool_id=EXCLUDED.pool_id,pair=EXCLUDED.pair,amm=EXCLUDED.amm,currency0=EXCLUDED.currency0,currency1=EXCLUDED.currency1,fee=EXCLUDED.fee,tick_spacing=EXCLUDED.tick_spacing,hooks=EXCLUDED.hooks,quote_token_address=EXCLUDED.quote_token_address,name=COALESCE(EXCLUDED.name,monitor_tokens.name),symbol=COALESCE(EXCLUDED.symbol,monitor_tokens.symbol),token_logo_url=COALESCE(EXCLUDED.token_logo_url,monitor_tokens.token_logo_url),quote_token_symbol=COALESCE(EXCLUDED.quote_token_symbol,monitor_tokens.quote_token_symbol),quote_token_logo_url=COALESCE($17,monitor_tokens.quote_token_logo_url),decimals=COALESCE(EXCLUDED.decimals,monitor_tokens.decimals),total_supply_raw=COALESCE(EXCLUDED.total_supply_raw,monitor_tokens.total_supply_raw),market_cap=COALESCE(EXCLUDED.market_cap,monitor_tokens.market_cap),token_price_eth=COALESCE(EXCLUDED.token_price_eth,monitor_tokens.token_price_eth),token_price_usd=COALESCE(EXCLUDED.token_price_usd,monitor_tokens.token_price_usd),price_change_5m=COALESCE(EXCLUDED.price_change_5m,monitor_tokens.price_change_5m),price_change_1h=COALESCE(EXCLUDED.price_change_1h,monitor_tokens.price_change_1h),price_change_24h=COALESCE(EXCLUDED.price_change_24h,monitor_tokens.price_change_24h),holders_count=COALESCE(EXCLUDED.holders_count,monitor_tokens.holders_count),buy_count_5m=COALESCE(EXCLUDED.buy_count_5m,monitor_tokens.buy_count_5m),sell_count_5m=COALESCE(EXCLUDED.sell_count_5m,monitor_tokens.sell_count_5m),volume_5m=COALESCE(EXCLUDED.volume_5m,monitor_tokens.volume_5m),quote_decimals=COALESCE(EXCLUDED.quote_decimals,monitor_tokens.quote_decimals),quote_usd_price=COALESCE(EXCLUDED.quote_usd_price,monitor_tokens.quote_usd_price),buy_tax=COALESCE(EXCLUDED.buy_tax,monitor_tokens.buy_tax),sell_tax=COALESCE(EXCLUDED.sell_tax,monitor_tokens.sell_tax),enabled=EXCLUDED.enabled,updated_at=now()
RETURNING ` + tokenSelect
	return scanToken(s.DB.QueryRow(ctx, q, tokenArgs(t)...))
}
func (s *Store) SetTokenEnabled(ctx context.Context, user int64, address string, enabled bool) error {
	r, e := s.DB.Exec(ctx, "UPDATE monitor_tokens SET enabled=$3,updated_at=now() WHERE user_id=$1 AND lower(token_address)=lower($2)", user, address, enabled)
	if e == nil && r.RowsAffected() == 0 {
		return fmt.Errorf("tracked token not found")
	}
	return e
}
func (s *Store) DeleteToken(ctx context.Context, user int64, address string) error {
	r, e := s.DB.Exec(ctx, "DELETE FROM monitor_tokens WHERE user_id=$1 AND lower(token_address)=lower($2)", user, address)
	if e == nil && r.RowsAffected() == 0 {
		return fmt.Errorf("tracked token not found")
	}
	return e
}
func (s *Store) SaveRoute(ctx context.Context, user int64, targetPool, source, dest, direction string, hops any, target string) (map[string]any, error) {
	b, _ := json.Marshal(hops)
	var sourceDecimals *int
	if strings.EqualFold(source, "0x0000000000000000000000000000000000000000") {
		n := 18
		sourceDecimals = &n
	} else {
		var decimals *int
		if err := s.DB.QueryRow(ctx, "SELECT decimals FROM monitor_tokens WHERE user_id=$1 AND lower(token_address)=lower($2) ORDER BY id DESC LIMIT 1", user, source).Scan(&decimals); err == nil {
			sourceDecimals = decimals
		}
	}
	var id int64
	err := s.DB.QueryRow(ctx, `INSERT INTO monitor_routes(user_id,target_token,target_pool_id,source_token,destination_token,direction,hops,source_token_decimals,last_verified_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,now()) ON CONFLICT(user_id,target_pool_id,source_token,destination_token,direction) DO UPDATE SET hops=EXCLUDED.hops,source_token_decimals=EXCLUDED.source_token_decimals,enabled=true,last_verified_at=now(),updated_at=now() RETURNING id`, user, target, targetPool, source, dest, direction, b, sourceDecimals).Scan(&id)
	return map[string]any{"id": id, "userId": user, "targetToken": target, "targetPoolId": targetPool, "sourceToken": source, "destinationToken": dest, "direction": direction, "hops": hops, "sourceTokenDecimals": sourceDecimals, "enabled": true}, err
}
func (s *Store) Routes(ctx context.Context, user int64) ([]map[string]any, error) {
	rows, e := s.DB.Query(ctx, "SELECT id,target_token,target_pool_id,source_token,destination_token,direction,hops,source_token_decimals,target_quote_price_eth,enabled,last_verified_at FROM monitor_routes WHERE user_id=$1 AND enabled=true ORDER BY id", user)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	var out []map[string]any
	for rows.Next() {
		var id int64
		var target, pool, src, dst, dir string
		var hops []byte
		var sourceDecimals *int
		var targetQuotePrice *string
		var en bool
		var verified *time.Time
		if e = rows.Scan(&id, &target, &pool, &src, &dst, &dir, &hops, &sourceDecimals, &targetQuotePrice, &en, &verified); e != nil {
			return nil, e
		}
		var h any
		_ = json.Unmarshal(hops, &h)
		out = append(out, map[string]any{"id": id, "userId": user, "targetToken": target, "targetPoolId": pool, "sourceToken": src, "destinationToken": dst, "direction": dir, "hops": h, "sourceTokenDecimals": sourceDecimals, "targetQuotePriceEth": targetQuotePrice, "enabled": en, "lastVerifiedAt": verified})
	}
	return out, rows.Err()
}
func (s *Store) DeleteRoute(ctx context.Context, user int64, pool, source, dest string) error {
	r, e := s.DB.Exec(ctx, "UPDATE monitor_routes SET enabled=false,updated_at=now() WHERE user_id=$1 AND target_pool_id=$2 AND enabled=true AND ((source_token=$3 AND destination_token=$4) OR (source_token=$4 AND destination_token=$3))", user, pool, source, dest)
	if e == nil && r.RowsAffected() == 0 {
		return fmt.Errorf("cached monitor route not found")
	}
	return e
}
func normalizeAddress(v string) string { return strings.ToLower(strings.TrimSpace(v)) }
