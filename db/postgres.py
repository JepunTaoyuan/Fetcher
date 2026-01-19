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

        sql = """
            INSERT INTO hyperliquid_trades (
                wallet_address, trade_id, tx_hash, symbol, order_id,
                side, direction, price, quantity, fee, fee_token,
                realized_pnl, is_taker, position_before, executed_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
            )
            ON CONFLICT (wallet_address, trade_id) DO NOTHING
        """

        async with self.pool.acquire() as conn:
            inserted = 0
            for trade in trades:
                try:
                    result = await conn.execute(
                        sql,
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
                    # ON CONFLICT DO NOTHING 時，result 會是 "INSERT 0 0" 或 "INSERT 0 1"
                    if "INSERT 0 1" in result:
                        inserted += 1
                except Exception as e:
                    logger.error(f"插入 Hyperliquid 交易失敗: {e}, trade_id={trade.get('trade_id')}")

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

        sql = """
            INSERT INTO orderly_trades (
                wallet_address, account_id, trade_id, symbol, order_id,
                side, price, quantity, fee, fee_token,
                realized_pnl, is_taker, executed_at
            ) VALUES (
                $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13
            )
            ON CONFLICT (wallet_address, trade_id) DO NOTHING
        """

        async with self.pool.acquire() as conn:
            inserted = 0
            for trade in trades:
                try:
                    result = await conn.execute(
                        sql,
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
                    if "INSERT 0 1" in result:
                        inserted += 1
                except Exception as e:
                    logger.error(f"插入 Orderly 交易失敗: {e}, trade_id={trade.get('trade_id')}")

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
