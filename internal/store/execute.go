package store

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v5"
	"strings"
	"time"
)

// ExecuteLine is the reusable configuration for one execute workflow.  The
// config JSON intentionally stays extensible so existing Node line settings
// can be carried over without a schema migration for every new option.
type ExecuteLine struct {
	LineID         int64          `json:"line"`
	UserID         int64          `json:"userId,omitempty"` // legacy monitorUser field; execute no longer depends on it
	Name           string         `json:"name"`
	Config         map[string]any `json:"config"`
	BossAddress    string         `json:"bossAddress"`
	BossPrivateKey string         `json:"-"`
	Enabled        bool           `json:"enabled"`
	CreatedAt      time.Time      `json:"createdAt"`
	UpdatedAt      time.Time      `json:"updatedAt"`
}

type ExecuteBatch struct {
	EID           int64     `json:"eid"`
	LineID        int64     `json:"line"`
	Active        bool      `json:"active"`
	BossAddress   string    `json:"bossAddress"`
	WalletsExist  bool      `json:"walletsExist"`
	Status        string    `json:"status"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
	PrivateKeyEnc string    `json:"-"`
}

type ExecuteWallet struct {
	ID            int64     `json:"id"`
	EID           int64     `json:"eid"`
	WalletIndex   int       `json:"index"`
	Address       string    `json:"address"`
	Active        bool      `json:"active"`
	CreatedAt     time.Time `json:"createdAt"`
	UpdatedAt     time.Time `json:"updatedAt"`
	PrivateKeyEnc string    `json:"-"`
}

type ExecuteToken struct {
	TID               int64
	EID               int64
	LineID            int64
	TokenAddress      string
	PoolID            string
	QuoteTokenAddress string
	Currency0         string
	Currency1         string
	Fee               int
	TickSpacing       int
	Hooks             string
	RouteBuy          []map[string]any
	RouteSell         []map[string]any
	AMM               string
	Status            string
	BuyStatus         string
	BuyStartTime      *time.Time
	Metadata          map[string]any
	CreatedAt         time.Time
	UpdatedAt         time.Time
}

func executeJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func scanExecuteLine(row interface{ Scan(...any) error }) (ExecuteLine, error) {
	var out ExecuteLine
	var raw []byte
	var userID *int64
	var bossAddress, bossPrivateKey *string
	err := row.Scan(&out.LineID, &userID, &out.Name, &raw, &bossAddress, &bossPrivateKey, &out.Enabled, &out.CreatedAt, &out.UpdatedAt)
	if err == nil {
		if userID != nil {
			out.UserID = *userID
		}
		if bossAddress != nil {
			out.BossAddress = *bossAddress
		}
		if bossPrivateKey != nil {
			out.BossPrivateKey = *bossPrivateKey
		}
		_ = json.Unmarshal(raw, &out.Config)
		if out.Config == nil {
			out.Config = map[string]any{}
		}
	}
	return out, err
}

func (s *Store) ExecuteLine(ctx context.Context, lineID int64) (ExecuteLine, error) {
	return scanExecuteLine(s.DB.QueryRow(ctx, `SELECT line_id,user_id,name,config,boss_address,boss_private_key_encrypted,enabled,created_at,updated_at FROM execute_lines WHERE line_id=$1`, lineID))
}

func (s *Store) NextExecuteLineID(ctx context.Context) (int64, error) {
	var lineID int64
	err := s.DB.QueryRow(ctx, `SELECT COALESCE(MAX(line_id),0)+1 FROM execute_lines`).Scan(&lineID)
	return lineID, err
}

func (s *Store) ExecuteLines(ctx context.Context) ([]ExecuteLine, error) {
	rows, err := s.DB.Query(ctx, `SELECT line_id,user_id,name,config,boss_address,boss_private_key_encrypted,enabled,created_at,updated_at FROM execute_lines ORDER BY line_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := []ExecuteLine{}
	for rows.Next() {
		line, scanErr := scanExecuteLine(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		line.BossPrivateKey = ""
		result = append(result, line)
	}
	return result, rows.Err()
}

func (s *Store) UpsertExecuteLine(ctx context.Context, lineID, userID int64, name string, cfg map[string]any, enabled bool, bossAddress, bossPrivateKey string) (ExecuteLine, error) {
	if lineID <= 0 {
		return ExecuteLine{}, fmt.Errorf("line is required")
	}
	if cfg == nil {
		cfg = map[string]any{}
	}
	if name == "" {
		name = strings.TrimSpace(fmt.Sprint(cfg["name"]))
	}
	return scanExecuteLine(s.DB.QueryRow(ctx, `INSERT INTO execute_lines(line_id,user_id,name,config,boss_address,boss_private_key_encrypted,enabled) VALUES($1,NULLIF($2,0),$3,$4,NULLIF($5,''),NULLIF($6,''),$7)
ON CONFLICT(line_id) DO UPDATE SET user_id=EXCLUDED.user_id,name=EXCLUDED.name,config=EXCLUDED.config,boss_address=COALESCE(EXCLUDED.boss_address,execute_lines.boss_address),boss_private_key_encrypted=COALESCE(EXCLUDED.boss_private_key_encrypted,execute_lines.boss_private_key_encrypted),enabled=EXCLUDED.enabled,updated_at=now()
RETURNING line_id,user_id,name,config,boss_address,boss_private_key_encrypted,enabled,created_at,updated_at`, lineID, userID, name, executeJSON(cfg), bossAddress, bossPrivateKey, enabled))
}

func (s *Store) SetExecuteLineBoss(ctx context.Context, lineID int64, address, encrypted string) error {
	_, err := s.DB.Exec(ctx, `UPDATE execute_lines SET boss_address=$2,boss_private_key_encrypted=$3,updated_at=now() WHERE line_id=$1`, lineID, address, encrypted)
	return err
}

func (s *Store) DeleteExecuteLine(ctx context.Context, lineID int64) error {
	_, err := s.DB.Exec(ctx, `DELETE FROM execute_lines WHERE line_id=$1`, lineID)
	return err
}

func scanExecuteBatch(row interface{ Scan(...any) error }) (ExecuteBatch, error) {
	var out ExecuteBatch
	err := row.Scan(&out.EID, &out.LineID, &out.Active, &out.BossAddress, &out.PrivateKeyEnc, &out.WalletsExist, &out.Status, &out.CreatedAt, &out.UpdatedAt)
	return out, err
}

func (s *Store) ExecuteBatch(ctx context.Context, eid int64) (ExecuteBatch, error) {
	return scanExecuteBatch(s.DB.QueryRow(ctx, `SELECT eid,line_id,active,boss_address,private_key_encrypted,wallets_exist,status,created_at,updated_at FROM execute_batches WHERE eid=$1`, eid))
}

func (s *Store) ActiveExecuteBatch(ctx context.Context, lineID int64) (ExecuteBatch, error) {
	return scanExecuteBatch(s.DB.QueryRow(ctx, `SELECT eid,line_id,active,boss_address,private_key_encrypted,wallets_exist,status,created_at,updated_at FROM execute_batches WHERE line_id=$1 AND active=true`, lineID))
}

func (s *Store) CreateExecuteBatch(ctx context.Context, lineID int64, address, encrypted string) (ExecuteBatch, error) {
	tx, err := s.DB.Begin(ctx)
	if err != nil {
		return ExecuteBatch{}, err
	}
	defer tx.Rollback(ctx)
	if _, err = tx.Exec(ctx, `UPDATE execute_batches SET active=false,status='closed',updated_at=now() WHERE line_id=$1 AND active=true`, lineID); err != nil {
		return ExecuteBatch{}, err
	}
	var out ExecuteBatch
	err = tx.QueryRow(ctx, `INSERT INTO execute_batches(line_id,boss_address,private_key_encrypted) VALUES($1,$2,$3)
RETURNING eid,line_id,active,boss_address,private_key_encrypted,wallets_exist,status,created_at,updated_at`, lineID, address, encrypted).Scan(&out.EID, &out.LineID, &out.Active, &out.BossAddress, &out.PrivateKeyEnc, &out.WalletsExist, &out.Status, &out.CreatedAt, &out.UpdatedAt)
	if err != nil {
		return ExecuteBatch{}, err
	}
	if err = tx.Commit(ctx); err != nil {
		return ExecuteBatch{}, err
	}
	return out, nil
}

func (s *Store) SetExecuteBatchWalletsExist(ctx context.Context, eid int64, value bool) error {
	_, err := s.DB.Exec(ctx, `UPDATE execute_batches SET wallets_exist=$2,updated_at=now() WHERE eid=$1`, eid, value)
	return err
}

func scanExecuteWallet(row interface{ Scan(...any) error }) (ExecuteWallet, error) {
	var out ExecuteWallet
	err := row.Scan(&out.ID, &out.EID, &out.WalletIndex, &out.Address, &out.PrivateKeyEnc, &out.Active, &out.CreatedAt, &out.UpdatedAt)
	return out, err
}

func (s *Store) ExecuteWallets(ctx context.Context, eid int64, activeOnly bool) ([]ExecuteWallet, error) {
	q := `SELECT id,eid,wallet_index,address,private_key_encrypted,active,created_at,updated_at FROM execute_wallets WHERE eid=$1`
	if activeOnly {
		q += ` AND active=true`
	}
	q += ` ORDER BY wallet_index,id`
	rows, err := s.DB.Query(ctx, q, eid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ExecuteWallet{}
	for rows.Next() {
		item, scanErr := scanExecuteWallet(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) UpsertExecuteWallet(ctx context.Context, w ExecuteWallet) (ExecuteWallet, error) {
	return scanExecuteWallet(s.DB.QueryRow(ctx, `INSERT INTO execute_wallets(eid,wallet_index,address,private_key_encrypted,active) VALUES($1,$2,$3,$4,$5)
ON CONFLICT(eid,address) DO UPDATE SET wallet_index=EXCLUDED.wallet_index,private_key_encrypted=EXCLUDED.private_key_encrypted,active=EXCLUDED.active,updated_at=now()
RETURNING id,eid,wallet_index,address,private_key_encrypted,active,created_at,updated_at`, w.EID, w.WalletIndex, w.Address, w.PrivateKeyEnc, w.Active))
}

func (s *Store) InsertExecuteWallet(ctx context.Context, w ExecuteWallet) (ExecuteWallet, error) {
	return scanExecuteWallet(s.DB.QueryRow(ctx, `INSERT INTO execute_wallets(eid,wallet_index,address,private_key_encrypted,active) VALUES($1,$2,$3,$4,$5)
RETURNING id,eid,wallet_index,address,private_key_encrypted,active,created_at,updated_at`, w.EID, w.WalletIndex, w.Address, w.PrivateKeyEnc, w.Active))
}

func (s *Store) DeactivateExecuteWallet(ctx context.Context, eid int64, address string) error {
	_, err := s.DB.Exec(ctx, `UPDATE execute_wallets SET active=false,updated_at=now() WHERE eid=$1 AND lower(address)=lower($2)`, eid, address)
	return err
}

const executeTokenSelect = `tid,eid,line_id,token_address,pool_id,quote_token_address,currency0,currency1,fee,tick_spacing,hooks,route_buy,route_sell,amm,status,buy_status,buy_start_time,metadata,created_at,updated_at`

func scanExecuteToken(row interface{ Scan(...any) error }) (ExecuteToken, error) {
	var out ExecuteToken
	var buyRaw, sellRaw, metaRaw []byte
	err := row.Scan(&out.TID, &out.EID, &out.LineID, &out.TokenAddress, &out.PoolID, &out.QuoteTokenAddress, &out.Currency0, &out.Currency1, &out.Fee, &out.TickSpacing, &out.Hooks, &buyRaw, &sellRaw, &out.AMM, &out.Status, &out.BuyStatus, &out.BuyStartTime, &metaRaw, &out.CreatedAt, &out.UpdatedAt)
	if err == nil {
		_ = json.Unmarshal(buyRaw, &out.RouteBuy)
		_ = json.Unmarshal(sellRaw, &out.RouteSell)
		_ = json.Unmarshal(metaRaw, &out.Metadata)
		if out.RouteBuy == nil {
			out.RouteBuy = []map[string]any{}
		}
		if out.RouteSell == nil {
			out.RouteSell = []map[string]any{}
		}
		if out.Metadata == nil {
			out.Metadata = map[string]any{}
		}
	}
	return out, err
}

func (t ExecuteToken) Map() map[string]any {
	return map[string]any{
		"tid": t.TID, "eid": t.EID, "line": t.LineID, "token": t.TokenAddress,
		"pool": t.PoolID, "poolId": t.PoolID, "quoteTokenAddress": t.QuoteTokenAddress,
		"currency0": t.Currency0, "currency1": t.Currency1, "fee": t.Fee,
		"tickSpacing": t.TickSpacing, "hooks": t.Hooks, "amm": t.AMM,
		"routeBuy": t.RouteBuy, "routeSell": t.RouteSell, "status": t.Status,
		"buyStatus": t.BuyStatus, "buyStartTime": t.BuyStartTime, "metadata": t.Metadata,
		"createdAt": t.CreatedAt, "updatedAt": t.UpdatedAt,
	}
}

func (s *Store) ExecuteToken(ctx context.Context, tid int64) (ExecuteToken, error) {
	return scanExecuteToken(s.DB.QueryRow(ctx, `SELECT `+executeTokenSelect+` FROM execute_tokens WHERE tid=$1`, tid))
}

func (s *Store) ExecuteTokenByPool(ctx context.Context, eid int64, token, pool string) (ExecuteToken, error) {
	return scanExecuteToken(s.DB.QueryRow(ctx, `SELECT `+executeTokenSelect+` FROM execute_tokens WHERE eid=$1 AND lower(token_address)=lower($2) AND lower(pool_id)=lower($3)`, eid, token, pool))
}

func (s *Store) ActiveExecuteTokenByAddress(ctx context.Context, eid int64, token string) (ExecuteToken, error) {
	return scanExecuteToken(s.DB.QueryRow(ctx, `SELECT `+executeTokenSelect+` FROM execute_tokens WHERE eid<>$1 AND lower(token_address)=lower($2) AND status='buy' ORDER BY updated_at DESC LIMIT 1`, eid, token))
}

func (s *Store) ExecuteTokens(ctx context.Context, eid int64) ([]ExecuteToken, error) {
	rows, err := s.DB.Query(ctx, `SELECT `+executeTokenSelect+` FROM execute_tokens WHERE eid=$1 ORDER BY tid`, eid)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ExecuteToken{}
	for rows.Next() {
		item, scanErr := scanExecuteToken(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) UpsertExecuteToken(ctx context.Context, t ExecuteToken) (ExecuteToken, error) {
	return scanExecuteToken(s.DB.QueryRow(ctx, `INSERT INTO execute_tokens(eid,line_id,token_address,pool_id,quote_token_address,currency0,currency1,fee,tick_spacing,hooks,route_buy,route_sell,amm,status,buy_status,buy_start_time,metadata)
VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)
ON CONFLICT(eid,token_address,pool_id) DO UPDATE SET line_id=EXCLUDED.line_id,quote_token_address=EXCLUDED.quote_token_address,currency0=EXCLUDED.currency0,currency1=EXCLUDED.currency1,fee=EXCLUDED.fee,tick_spacing=EXCLUDED.tick_spacing,hooks=EXCLUDED.hooks,route_buy=EXCLUDED.route_buy,route_sell=EXCLUDED.route_sell,amm=EXCLUDED.amm,metadata=EXCLUDED.metadata,updated_at=now()
RETURNING `+executeTokenSelect, t.EID, t.LineID, t.TokenAddress, t.PoolID, t.QuoteTokenAddress, t.Currency0, t.Currency1, t.Fee, t.TickSpacing, t.Hooks, executeJSON(t.RouteBuy), executeJSON(t.RouteSell), t.AMM, t.Status, t.BuyStatus, t.BuyStartTime, executeJSON(t.Metadata)))
}

func (s *Store) UpdateExecuteTokenStatus(ctx context.Context, tid int64, status, buyStatus string) error {
	_, err := s.DB.Exec(ctx, `UPDATE execute_tokens SET status=$2,buy_status=$3,buy_start_time=CASE WHEN $3 <> '' THEN now() ELSE buy_start_time END,updated_at=now() WHERE tid=$1`, tid, status, buyStatus)
	return err
}

func (s *Store) RecordExecuteTrade(ctx context.Context, tid, eid int64, wallet, side, stage, amount, minimum string) (int64, error) {
	var id int64
	err := s.DB.QueryRow(ctx, `INSERT INTO execute_trades(tid,eid,wallet_address,side,stage,amount_in_raw,amount_out_minimum_raw) VALUES($1,$2,$3,$4,$5,$6,$7) RETURNING id`, tid, eid, wallet, side, stage, amount, minimum).Scan(&id)
	return id, err
}

func (s *Store) FinishExecuteTrade(ctx context.Context, tradeID int64, status, hash, message string) error {
	_, err := s.DB.Exec(ctx, `UPDATE execute_trades SET status=$2,transaction_hash=NULLIF($3,''),error=NULLIF($4,''),updated_at=now() WHERE id=$1`, tradeID, status, hash, message)
	return err
}

func (s *Store) DeletePendingExecuteToken(ctx context.Context, tid int64) error {
	result, err := s.DB.Exec(ctx, `DELETE FROM execute_tokens WHERE tid=$1 AND status='pending'`, tid)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return pgx.ErrNoRows
	}
	return nil
}
