# Fetcher

Multi-platform trade data fetcher for **Hyperliquid** and **Orderly** exchanges. Fetches trading records by wallet address and stores them in PostgreSQL.

## Quick Start

```bash
pip install -r requirements.txt
cp .env.example .env   # edit with your credentials
python fetch_trades.py
```

## Configuration

`.env` file:

```env
# MongoDB (user data source)
MONGODB_URI=mongodb://localhost:27017
DATABASE_NAME=dexweb

# PostgreSQL (trade storage)
POSTGRES_HOST=interchange.proxy.rlwy.net
POSTGRES_PORT=22589
POSTGRES_DB=railway
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password

# Fetch settings
FETCH_DELAY_SECONDS=0.5
```

## Fetch Commands

```bash
python fetch_trades.py                          # all platforms, all users
python fetch_trades.py --platform hyperliquid   # Hyperliquid only
python fetch_trades.py --platform orderly       # Orderly only
python fetch_trades.py --wallet 0x31ca...       # specific wallet only
```

## Database Schema

### `hyperliquid_trades`

| Column | Type | Description |
|--------|------|-------------|
| `wallet_address` | `VARCHAR(42)` | User wallet address |
| `trade_id` | `BIGINT` | Unique trade ID (`tid`) |
| `tx_hash` | `VARCHAR(66)` | Transaction hash |
| `symbol` | `VARCHAR(32)` | Coin name, e.g. `BTC`, `ETH`, `SOL` |
| `order_id` | `BIGINT` | Order ID (`oid`) |
| `side` | `VARCHAR(4)` | `BUY` or `SELL` |
| `direction` | `VARCHAR(32)` | `Open Long`, `Close Short`, `Long > Short`, etc. |
| `price` | `DECIMAL(32,16)` | Execution price |
| `quantity` | `DECIMAL(32,16)` | Trade size |
| `fee` | `DECIMAL(32,16)` | Trading fee |
| `fee_token` | `VARCHAR(16)` | Fee token, e.g. `USDC` |
| `realized_pnl` | `DECIMAL(32,16)` | Closed PnL |
| `is_taker` | `BOOLEAN` | Whether trade was a taker order |
| `position_before` | `DECIMAL(32,16)` | Position size before this trade |
| `executed_at` | `TIMESTAMPTZ` | Execution time (UTC) |
| `created_at` | `TIMESTAMPTZ` | Row insertion time |

**Unique constraint:** `(wallet_address, trade_id)`

### `orderly_trades`

| Column | Type | Description |
|--------|------|-------------|
| `wallet_address` | `VARCHAR(42)` | User wallet address |
| `account_id` | `VARCHAR(128)` | Orderly account ID |
| `trade_id` | `BIGINT` | Unique trade ID |
| `symbol` | `VARCHAR(32)` | Trading pair, e.g. `PERP_ETH_USDC` |
| `order_id` | `BIGINT` | Order ID |
| `side` | `VARCHAR(4)` | `BUY` or `SELL` |
| `price` | `DECIMAL(32,16)` | Execution price |
| `quantity` | `DECIMAL(32,16)` | Trade size |
| `fee` | `DECIMAL(32,16)` | Trading fee |
| `fee_token` | `VARCHAR(16)` | Fee asset |
| `realized_pnl` | `DECIMAL(32,16)` | Realized PnL |
| `is_taker` | `BOOLEAN` | Whether trade was a taker order |
| `executed_at` | `TIMESTAMPTZ` | Execution time (UTC) |
| `created_at` | `TIMESTAMPTZ` | Row insertion time |

**Unique constraint:** `(account_id, trade_id)`

### `fetch_status`

Tracks incremental fetch progress per wallet per platform.

| Column | Type | Description |
|--------|------|-------------|
| `wallet_address` | `VARCHAR(42)` | Wallet address |
| `platform` | `VARCHAR(16)` | `hyperliquid` or `orderly` |
| `last_fetch_time` | `TIMESTAMPTZ` | Latest trade time from last fetch |
| `last_fetch_at` | `TIMESTAMPTZ` | When the fetch was executed |
| `total_trades_fetched` | `BIGINT` | Cumulative trades fetched |
| `last_error` | `TEXT` | Last error message (if any) |

## Querying the Data

### Common SQL Queries

**All trades for a wallet:**
```sql
SELECT * FROM hyperliquid_trades
WHERE wallet_address = '0x31ca8395cf837de08b24da3f660e77761dfb974b'
ORDER BY executed_at DESC;
```

**Daily trade count & volume per wallet:**
```sql
SELECT
    wallet_address,
    DATE(executed_at) AS trade_date,
    COUNT(*) AS trade_count,
    SUM(price * quantity) AS volume
FROM hyperliquid_trades
GROUP BY wallet_address, DATE(executed_at)
ORDER BY trade_date DESC;
```

**PnL summary per wallet:**
```sql
SELECT
    wallet_address,
    COUNT(*) AS total_trades,
    SUM(realized_pnl) AS total_pnl,
    SUM(fee) AS total_fees,
    MIN(executed_at) AS first_trade,
    MAX(executed_at) AS last_trade
FROM hyperliquid_trades
GROUP BY wallet_address;
```

**Trades by coin:**
```sql
SELECT
    symbol,
    COUNT(*) AS trades,
    SUM(quantity) AS total_size,
    SUM(realized_pnl) AS total_pnl
FROM hyperliquid_trades
WHERE wallet_address = '0x31ca8395cf837de08b24da3f660e77761dfb974b'
GROUP BY symbol
ORDER BY trades DESC;
```

**Win rate (trades with positive PnL):**
```sql
SELECT
    wallet_address,
    COUNT(*) FILTER (WHERE realized_pnl > 0) AS wins,
    COUNT(*) FILTER (WHERE realized_pnl < 0) AS losses,
    ROUND(
        100.0 * COUNT(*) FILTER (WHERE realized_pnl > 0) /
        NULLIF(COUNT(*) FILTER (WHERE realized_pnl != 0), 0), 2
    ) AS win_rate_pct
FROM hyperliquid_trades
GROUP BY wallet_address;
```

**Fetch status overview:**
```sql
SELECT * FROM fetch_status ORDER BY last_fetch_at DESC;
```

### Indexes

The following indexes are available for query performance:

- `idx_hl_trades_wallet` — filter by `wallet_address`
- `idx_hl_trades_executed_at` — filter/sort by time
- `idx_hl_trades_symbol` — filter by coin
- `idx_orderly_trades_wallet` — filter by `wallet_address`
- `idx_orderly_trades_account` — filter by `account_id`
- `idx_orderly_trades_executed_at` — filter/sort by time
- `idx_orderly_trades_symbol` — filter by symbol

## Project Structure

```
├── fetch_trades.py       # Main entry point (cron job)
├── read_users.py         # MongoDB user query utility
├── requirements.txt
├── db/
│   ├── postgres.py       # PostgreSQL connection & batch upsert
│   └── schema.sql        # DDL for all tables & indexes
├── fetchers/
│   ├── base.py           # Abstract base fetcher
│   ├── hyperliquid.py    # Hyperliquid API (with adaptive time splitting)
│   └── orderly.py        # Orderly API (paginated fetch)
└── models/
    └── trade.py          # Trade dataclasses & API response parsing
```

## Cron Job

```bash
0 3 * * * cd /path/to/Fetcher && python3 fetch_trades.py >> /var/log/fetch_trades.log 2>&1
```
