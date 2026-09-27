CREATE TABLE IF NOT EXISTS ave_configs (id INT PRIMARY KEY DEFAULT 1, x_auth TEXT NOT NULL, eth_usd_price NUMERIC, eth_usd_price_updated_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS monitor_users (id BIGSERIAL PRIMARY KEY, user_id BIGINT UNIQUE NOT NULL, wallet_address TEXT NOT NULL, private_key_encrypted TEXT, config JSONB NOT NULL DEFAULT '{}'::jsonb, enabled BOOLEAN NOT NULL DEFAULT true, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS monitor_users_enabled_idx ON monitor_users(enabled);
CREATE TABLE IF NOT EXISTS monitor_tokens (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL REFERENCES monitor_users(user_id) ON DELETE CASCADE, token_address TEXT NOT NULL, curve_address TEXT NOT NULL, pool_id TEXT NOT NULL, pair TEXT NOT NULL, amm TEXT, currency0 TEXT NOT NULL, currency1 TEXT NOT NULL, fee INT NOT NULL, tick_spacing INT NOT NULL, hooks TEXT NOT NULL, quote_token_address TEXT NOT NULL, name TEXT, symbol TEXT, token_logo_url TEXT, quote_token_symbol TEXT, quote_token_logo_url TEXT, decimals INT, total_supply_raw TEXT, market_cap TEXT, token_price_eth TEXT, token_price_usd TEXT, price_change_5m TEXT, price_change_1h TEXT, price_change_24h TEXT, holders_count BIGINT, buy_count_5m BIGINT, sell_count_5m BIGINT, volume_5m TEXT, quote_decimals INT, quote_usd_price TEXT, buy_tax TEXT, sell_tax TEXT, last_processed_block TEXT, enabled BOOLEAN NOT NULL DEFAULT true, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(user_id,token_address,curve_address));
CREATE INDEX IF NOT EXISTS monitor_tokens_pool_idx ON monitor_tokens(enabled,pool_id);
CREATE TABLE IF NOT EXISTS ave_token_pools (id BIGSERIAL PRIMARY KEY, token_address TEXT NOT NULL, pool_id TEXT NOT NULL, amm TEXT, quote_token_address TEXT NOT NULL, quote_token_symbol TEXT, quote_token_decimals INT, quote_price_eth TEXT, quote_price_usd TEXT, token_price_eth TEXT, token_price_usd TEXT, raw_data JSONB NOT NULL DEFAULT '{}'::jsonb, observed_at TIMESTAMPTZ NOT NULL DEFAULT now(), created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(token_address,pool_id));
CREATE TABLE IF NOT EXISTS monitor_routes (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, target_token TEXT NOT NULL, target_pool_id TEXT NOT NULL, source_token TEXT NOT NULL, destination_token TEXT NOT NULL, direction TEXT NOT NULL, hops JSONB NOT NULL, source_token_decimals INT, target_quote_price_eth TEXT, enabled BOOLEAN NOT NULL DEFAULT true, last_verified_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(user_id,target_pool_id,source_token,destination_token,direction));
CREATE TABLE IF NOT EXISTS monitor_records (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, token_address TEXT NOT NULL, curve_address TEXT NOT NULL, type TEXT NOT NULL, token_amount_raw TEXT NOT NULL, initial_token_amount_raw TEXT NOT NULL DEFAULT '0', quote_amount_raw TEXT NOT NULL, remain_token_amount_raw TEXT NOT NULL, remain_quote_amount_raw TEXT NOT NULL, remain_cost_usd_raw TEXT NOT NULL DEFAULT '0', average_cost_usd_raw TEXT NOT NULL DEFAULT '0', price_numerator TEXT, price_denominator TEXT, transaction_hash TEXT, reason TEXT, source_event_key TEXT UNIQUE, realized_quote_amount_raw TEXT NOT NULL DEFAULT '0', realized_pnl_quote_raw TEXT NOT NULL DEFAULT '0', gas_fee_native_raw TEXT NOT NULL DEFAULT '0', sell_count INT NOT NULL DEFAULT 0, profit_sell_level INT NOT NULL DEFAULT 0, scheduled_sell_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS strategy_positions (user_id BIGINT NOT NULL, token_address TEXT NOT NULL, curve_address TEXT NOT NULL, token_amount_raw NUMERIC NOT NULL DEFAULT 0, average_cost_usd NUMERIC NOT NULL DEFAULT 0, first_buy_price NUMERIC NOT NULL DEFAULT 0, last_buy_price NUMERIC NOT NULL DEFAULT 0, buy_count INT NOT NULL DEFAULT 0, sell_count INT NOT NULL DEFAULT 0, profit_sell_level INT NOT NULL DEFAULT 0, first_bought_at TIMESTAMPTZ, next_scheduled_sell_at TIMESTAMPTZ, external_cooldown_until TIMESTAMPTZ, last_buy_at TIMESTAMPTZ, last_event_key TEXT, updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(user_id,token_address,curve_address));
ALTER TABLE strategy_positions ADD COLUMN IF NOT EXISTS quote_spent_raw TEXT NOT NULL DEFAULT '0';
ALTER TABLE strategy_positions ADD COLUMN IF NOT EXISTS cost_usd_raw TEXT NOT NULL DEFAULT '0';
CREATE TABLE IF NOT EXISTS strategy_events (source_event_key TEXT PRIMARY KEY, user_id BIGINT NOT NULL, token_address TEXT NOT NULL, curve_address TEXT NOT NULL, created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS strategy_pending_buys (user_id BIGINT NOT NULL, token_address TEXT NOT NULL, curve_address TEXT NOT NULL, quote_usd NUMERIC NOT NULL, source_event_key TEXT NOT NULL, replacement_count INT NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'pending', created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(user_id,token_address,curve_address));
ALTER TABLE strategy_pending_buys ADD COLUMN IF NOT EXISTS transaction_hash TEXT;
ALTER TABLE strategy_pending_buys ADD COLUMN IF NOT EXISTS nonce BIGINT;
ALTER TABLE strategy_pending_buys ADD COLUMN IF NOT EXISTS max_fee_per_gas NUMERIC;
ALTER TABLE strategy_pending_buys ADD COLUMN IF NOT EXISTS max_priority_fee_per_gas NUMERIC;
ALTER TABLE strategy_pending_buys ADD COLUMN IF NOT EXISTS amount_out_minimum_raw TEXT;
ALTER TABLE strategy_pending_buys ADD COLUMN IF NOT EXISTS winner_transaction_hash TEXT;
CREATE TABLE IF NOT EXISTS strategy_buy_attempts (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, token_address TEXT NOT NULL, curve_address TEXT NOT NULL, transaction_hash TEXT NOT NULL UNIQUE, nonce BIGINT, quote_usd NUMERIC NOT NULL DEFAULT 0, source_event_key TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
ALTER TABLE strategy_buy_attempts ADD COLUMN IF NOT EXISTS amount_out_minimum_raw TEXT;
CREATE INDEX IF NOT EXISTS strategy_buy_attempts_position_idx ON strategy_buy_attempts(user_id,token_address,curve_address,status);
CREATE INDEX IF NOT EXISTS strategy_positions_due_idx ON strategy_positions(next_scheduled_sell_at) WHERE next_scheduled_sell_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS monitor_records_position_idx ON monitor_records(user_id,token_address,curve_address,created_at);
CREATE TABLE IF NOT EXISTS strategy_pending_actions (transaction_hash TEXT PRIMARY KEY, user_id BIGINT NOT NULL, token_address TEXT NOT NULL, curve_address TEXT NOT NULL, side TEXT NOT NULL, reason TEXT NOT NULL, amount NUMERIC NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'pending', created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE INDEX IF NOT EXISTS strategy_pending_actions_position_idx ON strategy_pending_actions(user_id,token_address,curve_address,status);
CREATE TABLE IF NOT EXISTS strategy_sell_failures (user_id BIGINT NOT NULL, token_address TEXT NOT NULL, curve_address TEXT NOT NULL, reason TEXT NOT NULL, token_amount_raw TEXT NOT NULL, buy_count INT NOT NULL DEFAULT 0, sell_count INT NOT NULL DEFAULT 0, trigger_key TEXT NOT NULL DEFAULT '', created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), PRIMARY KEY(user_id,token_address,curve_address,reason));
CREATE INDEX IF NOT EXISTS strategy_sell_failures_position_idx ON strategy_sell_failures(user_id,token_address,curve_address);
CREATE TABLE IF NOT EXISTS pending_buys (id BIGSERIAL PRIMARY KEY, user_id BIGINT NOT NULL, token_address TEXT NOT NULL, curve_address TEXT NOT NULL, transaction_hash TEXT NOT NULL, nonce BIGINT NOT NULL, quote_in_raw TEXT NOT NULL, amount_out_minimum_raw TEXT NOT NULL, route_json JSONB, source_event_key TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending', replacement_count INT NOT NULL DEFAULT 0, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now(), UNIQUE(user_id,token_address,curve_address));
CREATE TABLE IF NOT EXISTS files (id BIGSERIAL PRIMARY KEY, filename TEXT NOT NULL, original_name TEXT NOT NULL, mime_type TEXT NOT NULL, size BIGINT NOT NULL, driver TEXT NOT NULL, path TEXT NOT NULL, url TEXT, is_public BOOLEAN NOT NULL DEFAULT false, deleted_at TIMESTAMPTZ, created_at TIMESTAMPTZ NOT NULL DEFAULT now(), updated_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE IF NOT EXISTS app_logs (id BIGSERIAL PRIMARY KEY, level TEXT NOT NULL, message TEXT NOT NULL, context JSONB, created_at TIMESTAMPTZ NOT NULL DEFAULT now());

-- Execute workflow tables.  A line is a reusable execution configuration,
-- while a batch (eid) isolates the wallets and token jobs for one run.
CREATE TABLE IF NOT EXISTS execute_lines (
    line_id BIGINT PRIMARY KEY,
    user_id BIGINT,
    name TEXT NOT NULL DEFAULT '',
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    boss_address TEXT,
    boss_private_key_encrypted TEXT,
    enabled BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS execute_lines_user_idx ON execute_lines(user_id, enabled);
ALTER TABLE execute_lines DROP CONSTRAINT IF EXISTS execute_lines_user_id_fkey;
ALTER TABLE execute_lines ALTER COLUMN user_id DROP NOT NULL;
ALTER TABLE execute_lines ADD COLUMN IF NOT EXISTS boss_address TEXT;
ALTER TABLE execute_lines ADD COLUMN IF NOT EXISTS boss_private_key_encrypted TEXT;
CREATE TABLE IF NOT EXISTS execute_batches (
    eid BIGSERIAL PRIMARY KEY,
    line_id BIGINT NOT NULL REFERENCES execute_lines(line_id) ON DELETE CASCADE,
    active BOOLEAN NOT NULL DEFAULT true,
    boss_address TEXT NOT NULL,
    private_key_encrypted TEXT NOT NULL,
    wallets_exist BOOLEAN NOT NULL DEFAULT false,
    status TEXT NOT NULL DEFAULT 'active',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS execute_batches_active_line_idx ON execute_batches(line_id) WHERE active = true;
CREATE TABLE IF NOT EXISTS execute_wallets (
    id BIGSERIAL PRIMARY KEY,
    eid BIGINT NOT NULL REFERENCES execute_batches(eid) ON DELETE CASCADE,
    wallet_index INT NOT NULL,
    address TEXT NOT NULL,
    private_key_encrypted TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(eid, address)
);
CREATE INDEX IF NOT EXISTS execute_wallets_active_idx ON execute_wallets(eid, active, wallet_index);
-- A replaced wallet keeps its original index for audit and token-balance
-- recovery, so only the active flag identifies the current wallet.
ALTER TABLE execute_wallets DROP CONSTRAINT IF EXISTS execute_wallets_eid_wallet_index_key;
CREATE TABLE IF NOT EXISTS execute_tokens (
    tid BIGSERIAL PRIMARY KEY,
    eid BIGINT NOT NULL REFERENCES execute_batches(eid) ON DELETE CASCADE,
    line_id BIGINT NOT NULL REFERENCES execute_lines(line_id) ON DELETE CASCADE,
    token_address TEXT NOT NULL,
    pool_id TEXT NOT NULL,
    quote_token_address TEXT NOT NULL,
    currency0 TEXT NOT NULL,
    currency1 TEXT NOT NULL,
    fee INT NOT NULL,
    tick_spacing INT NOT NULL,
    hooks TEXT NOT NULL,
    route_buy JSONB NOT NULL DEFAULT '[]'::jsonb,
    route_sell JSONB NOT NULL DEFAULT '[]'::jsonb,
    amm TEXT NOT NULL DEFAULT 'uniswapv4',
    status TEXT NOT NULL DEFAULT 'pending',
    buy_status TEXT NOT NULL DEFAULT '',
    buy_start_time TIMESTAMPTZ,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE(eid, token_address, pool_id)
);
CREATE INDEX IF NOT EXISTS execute_tokens_batch_status_idx ON execute_tokens(eid, status, updated_at);
CREATE TABLE IF NOT EXISTS execute_trades (
    id BIGSERIAL PRIMARY KEY,
    tid BIGINT NOT NULL REFERENCES execute_tokens(tid) ON DELETE CASCADE,
    eid BIGINT NOT NULL REFERENCES execute_batches(eid) ON DELETE CASCADE,
    wallet_address TEXT NOT NULL,
    side TEXT NOT NULL,
    stage TEXT NOT NULL DEFAULT '',
    amount_in_raw TEXT NOT NULL,
    amount_out_minimum_raw TEXT NOT NULL DEFAULT '0',
    transaction_hash TEXT,
    status TEXT NOT NULL DEFAULT 'pending',
    error TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS execute_trades_tid_idx ON execute_trades(tid, created_at DESC);
