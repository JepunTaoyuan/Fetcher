-- Trade Fetcher PostgreSQL Schema
-- 用於儲存 Hyperliquid 和 Orderly 平台的交易紀錄

-- Hyperliquid 交易紀錄
CREATE TABLE IF NOT EXISTS hyperliquid_trades (
    id SERIAL PRIMARY KEY,

    -- 用戶識別
    wallet_address VARCHAR(42) NOT NULL,

    -- 交易識別 (用於去重)
    trade_id BIGINT NOT NULL,           -- Hyperliquid: tid
    tx_hash VARCHAR(66),                -- Hyperliquid: hash

    -- 交易資訊
    symbol VARCHAR(32) NOT NULL,        -- Hyperliquid: coin
    order_id BIGINT,                    -- Hyperliquid: oid
    side VARCHAR(4) NOT NULL,           -- 統一: "BUY" / "SELL"
    direction VARCHAR(32),              -- "Open Long", "Close Short", "Long > Short", etc.

    -- 價格與數量
    price DECIMAL(32, 16) NOT NULL,     -- Hyperliquid: px
    quantity DECIMAL(32, 16) NOT NULL,  -- Hyperliquid: sz

    -- 費用與盈虧
    fee DECIMAL(32, 16),                -- Hyperliquid: fee
    fee_token VARCHAR(16),              -- Hyperliquid: feeToken
    realized_pnl DECIMAL(32, 16),       -- Hyperliquid: closedPnl

    -- 額外資訊
    is_taker BOOLEAN,                   -- Hyperliquid: crossed
    position_before DECIMAL(32, 16),    -- Hyperliquid: startPosition

    -- 時間
    executed_at TIMESTAMP WITH TIME ZONE NOT NULL, -- Hyperliquid: time
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- 唯一約束 (去重)
    UNIQUE(wallet_address, trade_id)
);

-- Orderly 交易紀錄
CREATE TABLE IF NOT EXISTS orderly_trades (
    id SERIAL PRIMARY KEY,

    -- 用戶識別
    wallet_address VARCHAR(42) NOT NULL,
    account_id VARCHAR(128) NOT NULL,

    -- 交易識別 (用於去重)
    trade_id BIGINT NOT NULL,           -- Orderly: id / trade_id

    -- 交易資訊
    symbol VARCHAR(32) NOT NULL,        -- Orderly: symbol
    order_id BIGINT,                    -- Orderly: order_id
    side VARCHAR(4) NOT NULL,           -- 統一: "BUY" / "SELL"

    -- 價格與數量
    price DECIMAL(32, 16) NOT NULL,     -- Orderly: executed_price
    quantity DECIMAL(32, 16) NOT NULL,  -- Orderly: executed_quantity

    -- 費用與盈虧
    fee DECIMAL(32, 16),                -- Orderly: fee
    fee_token VARCHAR(16),              -- Orderly: fee_asset
    realized_pnl DECIMAL(32, 16),       -- Orderly: realized_pnl

    -- 額外資訊
    is_taker BOOLEAN,                   -- Orderly: NOT is_maker

    -- 時間
    executed_at TIMESTAMP WITH TIME ZONE NOT NULL, -- Orderly: created_time
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- 唯一約束 (去重，同一 account 的 trade_id 不重複)
    UNIQUE(account_id, trade_id)
);

-- 抓取狀態追蹤表 (用於增量抓取)
CREATE TABLE IF NOT EXISTS fetch_status (
    id SERIAL PRIMARY KEY,
    wallet_address VARCHAR(42) NOT NULL,
    platform VARCHAR(16) NOT NULL,      -- "hyperliquid" / "orderly"
    last_fetch_time TIMESTAMP WITH TIME ZONE, -- 上次抓取的最新交易時間
    last_fetch_at TIMESTAMP WITH TIME ZONE,   -- 上次執行抓取的時間
    total_trades_fetched BIGINT DEFAULT 0,
    last_error TEXT,

    UNIQUE(wallet_address, platform)
);

-- 索引
CREATE INDEX IF NOT EXISTS idx_hl_trades_wallet ON hyperliquid_trades(wallet_address);
CREATE INDEX IF NOT EXISTS idx_hl_trades_executed_at ON hyperliquid_trades(executed_at);
CREATE INDEX IF NOT EXISTS idx_hl_trades_symbol ON hyperliquid_trades(symbol);

CREATE INDEX IF NOT EXISTS idx_orderly_trades_wallet ON orderly_trades(wallet_address);
CREATE INDEX IF NOT EXISTS idx_orderly_trades_account ON orderly_trades(account_id);
CREATE INDEX IF NOT EXISTS idx_orderly_trades_executed_at ON orderly_trades(executed_at);
CREATE INDEX IF NOT EXISTS idx_orderly_trades_symbol ON orderly_trades(symbol);

CREATE INDEX IF NOT EXISTS idx_fetch_status_wallet_platform ON fetch_status(wallet_address, platform);
