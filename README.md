# Fetcher

Multi-platform trade data fetcher for Hyperliquid and Orderly exchanges.

This service fetches trading records from user wallets on multiple platforms and stores them in PostgreSQL for unified analysis and reporting.

## Features

- 🔄 **Multi-Platform Support**: Fetches trades from Hyperliquid and Orderly
- 📊 **Incremental Sync**: Tracks fetch status to only retrieve new trades
- 🗄️ **Unified Storage**: Stores all trades in PostgreSQL with consistent schema
- 👤 **User Management**: Reads user data from MongoDB referral system
- ⏰ **Cron Job Ready**: Designed for scheduled execution

## Architecture

```
fetcher/
├── fetch_trades.py      # Main cron job script
├── read_users.py        # MongoDB user query utility
├── requirements.txt     # Python dependencies
├── .env.example         # Environment variables template
├── db/
│   ├── postgres.py      # PostgreSQL connection manager
│   └── schema.sql       # Database schema
├── fetchers/
│   ├── base.py          # Abstract base fetcher class
│   ├── hyperliquid.py   # Hyperliquid API fetcher
│   └── orderly.py       # Orderly API fetcher
└── models/
    └── trade.py         # Trade dataclass definitions
```

## Prerequisites

- Python 3.9+
- MongoDB (for user data)
- PostgreSQL (for trade storage)

## Installation

1. Create and activate virtual environment:

```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
.\venv\Scripts\activate   # Windows
```

2. Install dependencies:

```bash
pip install -r requirements.txt
```

3. Configure environment variables:

```bash
cp .env.example .env
# Edit .env with your credentials
```

## Configuration

Create a `.env` file with the following variables:

```env
# MongoDB (user data source)
MONGODB_URI=mongodb://localhost:27017
DATABASE_NAME=referral_system

# PostgreSQL (trade storage)
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=trades_db
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password_here

# Fetch settings
FETCH_DELAY_SECONDS=0.5
```

## Usage

### Fetch Trades

```bash
# Fetch from all platforms for all users
python fetch_trades.py

# Fetch only from Hyperliquid
python fetch_trades.py --platform hyperliquid

# Fetch only from Orderly
python fetch_trades.py --platform orderly

# Fetch for a specific wallet
python fetch_trades.py --wallet 0x742d35b8...
```

### Query Users (Utility)

```bash
# Interactive menu
python read_users.py

# List all users
python read_users.py --all

# Query by user ID
python read_users.py --id USER_ID

# Query by wallet address
python read_users.py --wallet 0x742d35b8...

# List all affiliates
python read_users.py --affiliates

# Query referrals for an affiliate
python read_users.py --referrals AFFILIATE_ID
```

## Cron Job Setup

Add to crontab for daily execution at 3:00 AM:

```bash
0 3 * * * cd /home/worker/orderly/fetcher && /usr/bin/python3 fetch_trades.py >> /var/log/fetch_trades.log 2>&1
```

## Database Schema

### Tables

| Table | Description |
|-------|-------------|
| `hyperliquid_trades` | Trades fetched from Hyperliquid |
| `orderly_trades` | Trades fetched from Orderly |
| `fetch_status` | Tracking table for incremental sync |

### Hyperliquid Trade Fields

| Field | Type | Description |
|-------|------|-------------|
| `wallet_address` | VARCHAR(42) | User wallet address |
| `trade_id` | BIGINT | Unique trade ID (tid) |
| `tx_hash` | VARCHAR(66) | Transaction hash |
| `symbol` | VARCHAR(32) | Trading pair (coin) |
| `side` | VARCHAR(4) | BUY / SELL |
| `direction` | VARCHAR(10) | LONG / SHORT |
| `price` | DECIMAL | Execution price |
| `quantity` | DECIMAL | Trade size |
| `fee` | DECIMAL | Transaction fee |
| `realized_pnl` | DECIMAL | Closed PnL |
| `executed_at` | TIMESTAMP | Execution time |

### Orderly Trade Fields

| Field | Type | Description |
|-------|------|-------------|
| `wallet_address` | VARCHAR(42) | User wallet address |
| `account_id` | VARCHAR(128) | Orderly account ID |
| `trade_id` | BIGINT | Unique trade ID |
| `symbol` | VARCHAR(32) | Trading pair |
| `side` | VARCHAR(4) | BUY / SELL |
| `price` | DECIMAL | Execution price |
| `quantity` | DECIMAL | Trade size |
| `fee` | DECIMAL | Transaction fee |
| `realized_pnl` | DECIMAL | Realized PnL |
| `executed_at` | TIMESTAMP | Execution time |

## Dependencies

| Package | Purpose |
|---------|---------|
| `motor` | Async MongoDB driver |
| `asyncpg` | Async PostgreSQL driver |
| `python-dotenv` | Environment variable loading |
| `hyperliquid-python-sdk` | Hyperliquid API client |
| `orderly-evm-connector-python` | Orderly API client |

## How It Works

1. **Read Users**: Fetches user list from MongoDB `users` collection
2. **Check Status**: Retrieves last fetch timestamp from `fetch_status` table
3. **Fetch Trades**: Calls platform APIs with incremental time range
4. **Transform**: Converts API responses to unified `Trade` dataclass
5. **Store**: Upserts trades into PostgreSQL (deduplication via unique constraints)
6. **Update Status**: Records latest trade timestamp for next incremental fetch

## Extending

### Adding a New Platform

1. Create a new fetcher in `fetchers/`:

```python
from .base import BaseFetcher

class NewPlatformFetcher(BaseFetcher):
    platform_name = "new_platform"
    
    async def fetch_trades(self, wallet_address, start_time, end_time, **kwargs):
        # Implement API calls
        pass
    
    async def close(self):
        pass
```

2. Add trade model in `models/trade.py`
3. Add database table in `db/schema.sql`
4. Update `db/postgres.py` with upsert methods
5. Integrate in `fetch_trades.py`

## License

MIT
