"""
PostgreSQL 連線管理與資料操作
"""

import asyncio
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Dict, Any
from decimal import Decimal

import asyncpg

logger = logging.getLogger(__name__)


class PostgresManager:
    """PostgreSQL 資料庫管理器"""

    def __init__(
        self,
        host: str,
        port: int,
        database: str,
        user: str,
        password: str,
    ):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        """建立資料庫連接池"""
        try:
            self.pool = await asyncpg.create_pool(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password,
                min_size=2,
                max_size=10,
            )
            logger.info(f"PostgreSQL 連接成功: {self.host}:{self.port}/{self.database}")
        except Exception as e:
            logger.error(f"PostgreSQL 連接失敗: {e}")
            raise

    async def disconnect(self):
        """關閉資料庫連接池"""
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL 連接已關閉")

    async def init_schema(self):
        """初始化資料庫 schema"""
        schema_path = Path(__file__).parent / "schema.sql"

        if not schema_path.exists():
            logger.warning(f"Schema 檔案不存在: {schema_path}")
            return

        async with self.pool.acquire() as conn:
            with open(schema_path, "r") as f:
                schema_sql = f.read()
            await conn.execute(schema_sql)
            logger.info("PostgreSQL schema 初始化完成")

    # ==================== Hyperliquid Trades ====================

    async def upsert_hyperliquid_trades(
        self, trades: List[Dict[str, Any]]
    ) -> int:
        """
        批量插入 Hyperliquid 交易紀錄 (UPSERT)

        Args:
            trades: 交易紀錄列表

        Returns:
            成功插入的紀錄數
        """
        if not trades:
            return 0

        rows = [
            (
                trade["wallet_address"],
                trade["trade_id"],
                trade.get("tx_hash"),
                trade["symbol"],
                trade.get("order_id"),
                trade["side"],
                trade.get("direction"),
                Decimal(str(trade["price"])),
                Decimal(str(trade["quantity"])),
                Decimal(str(trade["fee"])) if trade.get("fee") is not None else None,
                trade.get("fee_token"),
                Decimal(str(trade["realized_pnl"])) if trade.get("realized_pnl") is not None else None,
                trade.get("is_taker"),
                Decimal(str(trade["position_before"])) if trade.get("position_before") is not None else None,
                trade["executed_at"],
            )
            for trade in trades
        ]

        BATCH_SIZE = 500
        inserted = 0

        async with self.pool.acquire() as conn:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]
                try:
                    result = await conn.fetch(
                        """
                        INSERT INTO hyperliquid_trades (
                            wallet_address, trade_id, tx_hash, symbol, order_id,
                            side, direction, price, quantity, fee, fee_token,
                            realized_pnl, is_taker, position_before, executed_at
                        )
                        SELECT u.* FROM unnest(
                            $1::varchar[], $2::bigint[], $3::varchar[], $4::varchar[], $5::bigint[],
                            $6::varchar[], $7::varchar[], $8::decimal[], $9::decimal[], $10::decimal[], $11::varchar[],
                            $12::decimal[], $13::boolean[], $14::decimal[], $15::timestamptz[]
                        ) AS u(
                            wallet_address, trade_id, tx_hash, symbol, order_id,
                            side, direction, price, quantity, fee, fee_token,
                            realized_pnl, is_taker, position_before, executed_at
                        )
                        ON CONFLICT (wallet_address, trade_id) DO NOTHING
                        RETURNING trade_id
                        """,
                        [r[0] for r in batch],
                        [r[1] for r in batch],
                        [r[2] for r in batch],
                        [r[3] for r in batch],
                        [r[4] for r in batch],
                        [r[5] for r in batch],
                        [r[6] for r in batch],
                        [r[7] for r in batch],
                        [r[8] for r in batch],
                        [r[9] for r in batch],
                        [r[10] for r in batch],
                        [r[11] for r in batch],
                        [r[12] for r in batch],
                        [r[13] for r in batch],
                        [r[14] for r in batch],
                    )
                    inserted += len(result)
                    logger.info(f"批次寫入 Hyperliquid: {len(result)}/{len(batch)} 筆 (batch {i // BATCH_SIZE + 1})")
                except Exception as e:
                    logger.error(f"批次寫入 Hyperliquid 失敗: {e}")

        return inserted

    async def get_hyperliquid_trades_count(self, wallet_address: str) -> int:
        """取得指定錢包的 Hyperliquid 交易紀錄數"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) FROM hyperliquid_trades WHERE wallet_address = $1",
                wallet_address
            )
            return row[0] if row else 0

    # ==================== Orderly Trades ====================

    async def upsert_orderly_trades(
        self, trades: List[Dict[str, Any]]
    ) -> int:
        """
        批量插入 Orderly 交易紀錄 (UPSERT)

        Args:
            trades: 交易紀錄列表

        Returns:
            成功插入的紀錄數
        """
        if not trades:
            return 0

        rows = [
            (
                trade["wallet_address"],
                trade["account_id"],
                trade["trade_id"],
                trade["symbol"],
                trade.get("order_id"),
                trade["side"],
                Decimal(str(trade["price"])),
                Decimal(str(trade["quantity"])),
                Decimal(str(trade["fee"])) if trade.get("fee") is not None else None,
                trade.get("fee_token"),
                Decimal(str(trade["realized_pnl"])) if trade.get("realized_pnl") is not None else None,
                trade.get("is_taker"),
                trade["executed_at"],
            )
            for trade in trades
        ]

        BATCH_SIZE = 500
        inserted = 0

        async with self.pool.acquire() as conn:
            for i in range(0, len(rows), BATCH_SIZE):
                batch = rows[i:i + BATCH_SIZE]
                try:
                    result = await conn.fetch(
                        """
                        INSERT INTO orderly_trades (
                            wallet_address, account_id, trade_id, symbol, order_id,
                            side, price, quantity, fee, fee_token,
                            realized_pnl, is_taker, executed_at
                        )
                        SELECT u.* FROM unnest(
                            $1::varchar[], $2::varchar[], $3::bigint[], $4::varchar[], $5::bigint[],
                            $6::varchar[], $7::decimal[], $8::decimal[], $9::decimal[], $10::varchar[],
                            $11::decimal[], $12::boolean[], $13::timestamptz[]
                        ) AS u(
                            wallet_address, account_id, trade_id, symbol, order_id,
                            side, price, quantity, fee, fee_token,
                            realized_pnl, is_taker, executed_at
                        )
                        ON CONFLICT (account_id, trade_id) DO NOTHING
                        RETURNING trade_id
                        """,
                        [r[0] for r in batch],
                        [r[1] for r in batch],
                        [r[2] for r in batch],
                        [r[3] for r in batch],
                        [r[4] for r in batch],
                        [r[5] for r in batch],
                        [r[6] for r in batch],
                        [r[7] for r in batch],
                        [r[8] for r in batch],
                        [r[9] for r in batch],
                        [r[10] for r in batch],
                        [r[11] for r in batch],
                        [r[12] for r in batch],
                    )
                    inserted += len(result)
                    logger.info(f"批次寫入 Orderly: {len(result)}/{len(batch)} 筆 (batch {i // BATCH_SIZE + 1})")
                except Exception as e:
                    logger.error(f"批次寫入 Orderly 失敗: {e}")

        return inserted

    async def get_orderly_trades_count(self, wallet_address: str) -> int:
        """取得指定錢包的 Orderly 交易紀錄數"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT COUNT(*) FROM orderly_trades WHERE wallet_address = $1",
                wallet_address
            )
            return row[0] if row else 0

    # ==================== Fetch Status ====================

    async def get_fetch_status(
        self, wallet_address: str, platform: str
    ) -> Optional[Dict[str, Any]]:
        """
        取得抓取狀態

        Args:
            wallet_address: 錢包地址
            platform: 平台名稱 ("hyperliquid" / "orderly")

        Returns:
            抓取狀態或 None
        """
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                SELECT wallet_address, platform, last_fetch_time,
                       last_fetch_at, total_trades_fetched, last_error
                FROM fetch_status
                WHERE wallet_address = $1 AND platform = $2
                """,
                wallet_address,
                platform,
            )

            if row:
                return dict(row)
            return None

    async def upsert_fetch_status(
        self,
        wallet_address: str,
        platform: str,
        last_fetch_time: Optional[datetime] = None,
        total_trades_fetched: int = 0,
        last_error: Optional[str] = None,
    ):
        """
        更新或插入抓取狀態

        Args:
            wallet_address: 錢包地址
            platform: 平台名稱
            last_fetch_time: 上次抓取的最新交易時間
            total_trades_fetched: 總抓取交易數
            last_error: 最後錯誤訊息
        """
        sql = """
            INSERT INTO fetch_status (
                wallet_address, platform, last_fetch_time, last_fetch_at,
                total_trades_fetched, last_error
            ) VALUES ($1, $2, $3, NOW(), $4, $5)
            ON CONFLICT (wallet_address, platform)
            DO UPDATE SET
                last_fetch_time = COALESCE($3, fetch_status.last_fetch_time),
                last_fetch_at = NOW(),
                total_trades_fetched = fetch_status.total_trades_fetched + $4,
                last_error = $5
        """

        async with self.pool.acquire() as conn:
            await conn.execute(
                sql,
                wallet_address,
                platform,
                last_fetch_time,
                total_trades_fetched,
                last_error,
            )

    async def update_fetch_error(
        self, wallet_address: str, platform: str, error: str
    ):
        """更新抓取錯誤訊息"""
        await self.upsert_fetch_status(
            wallet_address=wallet_address,
            platform=platform,
            total_trades_fetched=0,
            last_error=error,
        )
