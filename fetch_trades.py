#!/usr/bin/env python3
"""
多平台交易紀錄抓取 Cron Job

從 MongoDB 讀取用戶資料，分別從 Hyperliquid 和 Orderly 兩個平台抓取交易紀錄，
並統一儲存到 PostgreSQL。

使用方式:
    python fetch_trades.py                    # 抓取所有平台
    python fetch_trades.py --platform hyperliquid  # 只抓取 Hyperliquid
    python fetch_trades.py --platform orderly      # 只抓取 Orderly
    python fetch_trades.py --wallet 0x742d35b8...  # 只抓取特定用戶

Cron Job 設定 (每天凌晨 3 點):
    0 3 * * * cd /home/worker/orderly/fetcher && /usr/bin/python3 fetch_trades.py >> /var/log/fetch_trades.log 2>&1
"""

import asyncio
import argparse
import logging
import os
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

from db.postgres import PostgresManager
from fetchers.hyperliquid import HyperliquidFetcher
from fetchers.orderly import OrderlyFetcher

# 載入環境變數
load_dotenv()

# 設定日誌
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# 預設開始抓取時間
DEFAULT_START_DATE = datetime(2025, 1, 1)

# 每個用戶間的延遲 (秒)
USER_DELAY_SECONDS = float(os.getenv("FETCH_DELAY_SECONDS", "0.5"))


class TradeFetcher:
    """交易紀錄抓取器主程式"""

    def __init__(
        self,
        mongodb_uri: str,
        mongodb_database: str,
        postgres_host: str,
        postgres_port: int,
        postgres_db: str,
        postgres_user: str,
        postgres_password: str,
    ):
        self.mongodb_uri = mongodb_uri
        self.mongodb_database = mongodb_database

        # MongoDB client
        self.mongo_client: Optional[AsyncIOMotorClient] = None
        self.mongo_db = None

        # PostgreSQL manager
        self.pg = PostgresManager(
            host=postgres_host,
            port=postgres_port,
            database=postgres_db,
            user=postgres_user,
            password=postgres_password,
        )

        # Fetchers
        self.hl_fetcher = HyperliquidFetcher()
        self.orderly_fetcher = OrderlyFetcher()

        # 統計資料
        self.stats = {
            "users_processed": 0,
            "hyperliquid_trades": 0,
            "orderly_trades": 0,
            "errors": 0,
        }

    async def connect(self):
        """建立資料庫連接"""
        # MongoDB
        self.mongo_client = AsyncIOMotorClient(self.mongodb_uri)
        self.mongo_db = self.mongo_client[self.mongodb_database]
        await self.mongo_client.admin.command("ping")
        logger.info(f"MongoDB 連接成功: {self.mongodb_database}")

        # PostgreSQL
        await self.pg.connect()
        await self.pg.init_schema()

    async def disconnect(self):
        """關閉資料庫連接"""
        if self.mongo_client:
            self.mongo_client.close()
            logger.info("MongoDB 連接已關閉")

        await self.pg.disconnect()
        await self.hl_fetcher.close()
        await self.orderly_fetcher.close()

    async def get_users(
        self, wallet_address: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """
        取得用戶列表

        Args:
            wallet_address: 指定錢包地址 (可選)

        Returns:
            用戶列表
        """
        query = {}
        if wallet_address:
            query["wallet_address"] = wallet_address

        cursor = self.mongo_db.users.find(query)
        users = await cursor.to_list(length=10000)
        logger.info(f"取得 {len(users)} 位用戶")
        return users

    async def fetch_hyperliquid_for_user(self, user: Dict[str, Any]) -> int:
        """
        為指定用戶抓取 Hyperliquid 交易紀錄

        Args:
            user: 用戶資料

        Returns:
            新增的交易紀錄數
        """
        wallet_address = user.get("wallet_address")
        if not wallet_address:
            logger.warning(f"用戶 {user.get('_id')} 無錢包地址，跳過 Hyperliquid")
            return 0

        try:
            # 取得上次抓取狀態
            status = await self.pg.get_fetch_status(wallet_address, "hyperliquid")

            if status and status.get("last_fetch_time"):
                start_time = status["last_fetch_time"]
                logger.info(
                    f"[Hyperliquid] 增量抓取 {wallet_address[:10]}... 從 {start_time}"
                )
            else:
                start_time = DEFAULT_START_DATE
                logger.info(
                    f"[Hyperliquid] 首次抓取 {wallet_address[:10]}... 從 {start_time}"
                )

            # 抓取交易
            trades = await self.hl_fetcher.fetch_trades(
                wallet_address=wallet_address,
                start_time=start_time,
                end_time=datetime.now(),
            )

            if not trades:
                logger.info(f"[Hyperliquid] {wallet_address[:10]}... 無新交易")
                return 0

            # 寫入 PostgreSQL
            inserted = await self.pg.upsert_hyperliquid_trades(trades)

            # 更新抓取狀態
            latest_time = max(t["executed_at"] for t in trades)
            await self.pg.upsert_fetch_status(
                wallet_address=wallet_address,
                platform="hyperliquid",
                last_fetch_time=latest_time,
                total_trades_fetched=inserted,
            )

            logger.info(
                f"[Hyperliquid] {wallet_address[:10]}... 完成，新增 {inserted}/{len(trades)} 筆"
            )
            return inserted

        except Exception as e:
            logger.error(f"[Hyperliquid] {wallet_address[:10]}... 錯誤: {e}")
            await self.pg.update_fetch_error(wallet_address, "hyperliquid", str(e))
            self.stats["errors"] += 1
            return 0

    async def fetch_orderly_for_user(self, user: Dict[str, Any]) -> int:
        """
        為指定用戶抓取 Orderly 交易紀錄

        Args:
            user: 用戶資料

        Returns:
            新增的交易紀錄數
        """
        wallet_address = user.get("wallet_address")
        orderly_key = user.get("user_api_key")
        orderly_secret = user.get("user_api_secret")
        account_id = user.get("_id")  # 使用 user_id 作為 account_id

        if not wallet_address:
            logger.warning(f"用戶 {user.get('_id')} 無錢包地址，跳過 Orderly")
            return 0

        if not orderly_key or not orderly_secret:
            logger.info(f"[Orderly] {wallet_address[:10]}... 無 API 憑證，跳過")
            return 0

        try:
            # 取得上次抓取狀態
            status = await self.pg.get_fetch_status(wallet_address, "orderly")

            if status and status.get("last_fetch_time"):
                start_time = status["last_fetch_time"]
                logger.info(
                    f"[Orderly] 增量抓取 {wallet_address[:10]}... 從 {start_time}"
                )
            else:
                start_time = DEFAULT_START_DATE
                logger.info(
                    f"[Orderly] 首次抓取 {wallet_address[:10]}... 從 {start_time}"
                )

            # 抓取交易
            trades = await self.orderly_fetcher.fetch_trades(
                wallet_address=wallet_address,
                start_time=start_time,
                end_time=datetime.now(),
                orderly_key=orderly_key,
                orderly_secret=orderly_secret,
                account_id=account_id,
            )

            if not trades:
                logger.info(f"[Orderly] {wallet_address[:10]}... 無新交易")
                return 0

            # 寫入 PostgreSQL
            inserted = await self.pg.upsert_orderly_trades(trades)

            # 更新抓取狀態
            latest_time = max(t["executed_at"] for t in trades)
            await self.pg.upsert_fetch_status(
                wallet_address=wallet_address,
                platform="orderly",
                last_fetch_time=latest_time,
                total_trades_fetched=inserted,
            )

            logger.info(
                f"[Orderly] {wallet_address[:10]}... 完成，新增 {inserted}/{len(trades)} 筆"
            )
            return inserted

        except Exception as e:
            logger.error(f"[Orderly] {wallet_address[:10]}... 錯誤: {e}")
            await self.pg.update_fetch_error(wallet_address, "orderly", str(e))
            self.stats["errors"] += 1
            return 0

    async def run(
        self,
        platform: Optional[str] = None,
        wallet_address: Optional[str] = None,
    ):
        """
        執行抓取任務

        Args:
            platform: 指定平台 ("hyperliquid" / "orderly")
            wallet_address: 指定錢包地址
        """
        start_time = datetime.now()
        logger.info("=" * 60)
        logger.info("開始執行交易紀錄抓取")
        logger.info(f"平台: {platform or '全部'}")
        logger.info(f"錢包: {wallet_address or '全部'}")
        logger.info("=" * 60)

        try:
            await self.connect()

            users = await self.get_users(wallet_address)

            for i, user in enumerate(users, 1):
                wallet = user.get("wallet_address", "N/A")
                logger.info(f"[{i}/{len(users)}] 處理用戶 {wallet[:10]}...")

                # Hyperliquid
                if platform is None or platform == "hyperliquid":
                    hl_count = await self.fetch_hyperliquid_for_user(user)
                    self.stats["hyperliquid_trades"] += hl_count

                # Orderly
                if platform is None or platform == "orderly":
                    orderly_count = await self.fetch_orderly_for_user(user)
                    self.stats["orderly_trades"] += orderly_count

                self.stats["users_processed"] += 1

                # 用戶間延遲
                if i < len(users):
                    await asyncio.sleep(USER_DELAY_SECONDS)

        finally:
            await self.disconnect()

        # 輸出統計
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.info("=" * 60)
        logger.info("抓取完成")
        logger.info(f"處理用戶數: {self.stats['users_processed']}")
        logger.info(f"Hyperliquid 新增交易: {self.stats['hyperliquid_trades']}")
        logger.info(f"Orderly 新增交易: {self.stats['orderly_trades']}")
        logger.info(f"錯誤數: {self.stats['errors']}")
        logger.info(f"執行時間: {elapsed:.2f} 秒")
        logger.info("=" * 60)


def parse_args() -> argparse.Namespace:
    """解析命令列參數"""
    parser = argparse.ArgumentParser(
        description="多平台交易紀錄抓取 Cron Job",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
    python fetch_trades.py                          # 抓取所有平台
    python fetch_trades.py --platform hyperliquid   # 只抓取 Hyperliquid
    python fetch_trades.py --platform orderly       # 只抓取 Orderly
    python fetch_trades.py --wallet 0x742d35b8...   # 只抓取特定用戶
        """,
    )

    parser.add_argument(
        "--platform",
        "-p",
        type=str,
        choices=["hyperliquid", "orderly"],
        help="指定要抓取的平台",
    )

    parser.add_argument(
        "--wallet",
        "-w",
        type=str,
        help="指定要抓取的錢包地址",
    )

    # MongoDB 設定
    parser.add_argument(
        "--mongodb-uri",
        type=str,
        default=os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
        help="MongoDB 連線 URI",
    )

    parser.add_argument(
        "--mongodb-database",
        type=str,
        default=os.getenv("DATABASE_NAME", "referral_system"),
        help="MongoDB 資料庫名稱",
    )

    # PostgreSQL 設定
    parser.add_argument(
        "--postgres-host",
        type=str,
        default=os.getenv("POSTGRES_HOST", "localhost"),
        help="PostgreSQL host",
    )

    parser.add_argument(
        "--postgres-port",
        type=int,
        default=int(os.getenv("POSTGRES_PORT", "5432")),
        help="PostgreSQL port",
    )

    parser.add_argument(
        "--postgres-db",
        type=str,
        default=os.getenv("POSTGRES_DB", "trades_db"),
        help="PostgreSQL database",
    )

    parser.add_argument(
        "--postgres-user",
        type=str,
        default=os.getenv("POSTGRES_USER", "postgres"),
        help="PostgreSQL user",
    )

    parser.add_argument(
        "--postgres-password",
        type=str,
        default=os.getenv("POSTGRES_PASSWORD", ""),
        help="PostgreSQL password",
    )

    return parser.parse_args()


async def main():
    """主程式入口"""
    args = parse_args()

    fetcher = TradeFetcher(
        mongodb_uri=args.mongodb_uri,
        mongodb_database=args.mongodb_database,
        postgres_host=args.postgres_host,
        postgres_port=args.postgres_port,
        postgres_db=args.postgres_db,
        postgres_user=args.postgres_user,
        postgres_password=args.postgres_password,
    )

    await fetcher.run(
        platform=args.platform,
        wallet_address=args.wallet,
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("操作已取消")
        sys.exit(0)
    except Exception as e:
        logger.error(f"執行失敗: {e}")
        sys.exit(1)
