"""
Hyperliquid 交易資料抓取器
"""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional

from hyperliquid.info import Info

from .base import BaseFetcher
from models.trade import HyperliquidTrade

logger = logging.getLogger(__name__)

# 每次抓取的時間區間 (天)
FETCH_INTERVAL_DAYS = 30

# 單次回傳上限，超過則需要縮短時間區間重試
FILLS_LIMIT = 500

# 最小時間區間 (小時)，避免無限遞迴
MIN_INTERVAL_HOURS = 1

# 最大重試次數
MAX_RETRIES = 3


class HyperliquidFetcher(BaseFetcher):
    """Hyperliquid 交易資料抓取器"""

    platform_name = "hyperliquid"

    def __init__(self, base_url: Optional[str] = None):
        """
        初始化 Hyperliquid 抓取器

        Args:
            base_url: API base URL (可選，預設使用官方 mainnet)
        """
        self.info = Info(base_url=base_url, skip_ws=True)
        self._log_info("Hyperliquid fetcher initialized")

    async def _fetch_fills_for_interval(
        self,
        wallet_address: str,
        interval_start: datetime,
        interval_end: datetime,
    ) -> List[Dict[str, Any]]:
        """
        抓取單一時間區間的 fills，若回傳量達上限則自動切分區間重試

        Args:
            wallet_address: 錢包地址
            interval_start: 區間開始時間 (UTC)
            interval_end: 區間結束時間 (UTC)

        Returns:
            fills 列表 (raw API response dicts)
        """
        start_ts = int(interval_start.timestamp() * 1000)
        end_ts = int(interval_end.timestamp() * 1000)

        loop = asyncio.get_running_loop()

        for attempt in range(MAX_RETRIES):
            try:
                fills = await loop.run_in_executor(
                    None,
                    lambda s=start_ts, e=end_ts: self.info.user_fills_by_time(
                        wallet_address, s, e
                    ),
                )
                break
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    self._log_warning(
                        f"抓取失敗，重試 {attempt + 1}/{MAX_RETRIES}",
                        wallet=wallet_address[:10] + "...",
                        error=str(e),
                    )
                    await asyncio.sleep(1 * (attempt + 1))
                else:
                    self._log_error(
                        f"抓取失敗，已達最大重試次數",
                        wallet=wallet_address[:10] + "...",
                        period=f"{interval_start.date()} ~ {interval_end.date()}",
                        error=str(e),
                    )
                    return []

        if not fills:
            return []

        # 若回傳量達上限且區間仍可切分，則遞迴切分
        if len(fills) >= FILLS_LIMIT:
            interval_hours = (interval_end - interval_start).total_seconds() / 3600
            if interval_hours > MIN_INTERVAL_HOURS:
                mid = interval_start + (interval_end - interval_start) / 2
                self._log_info(
                    f"回傳量達上限 ({len(fills)})，切分區間重試",
                    wallet=wallet_address[:10] + "...",
                    period=f"{interval_start.date()} ~ {interval_end.date()}",
                )
                first_half = await self._fetch_fills_for_interval(
                    wallet_address, interval_start, mid
                )
                await asyncio.sleep(0.2)
                second_half = await self._fetch_fills_for_interval(
                    wallet_address, mid, interval_end
                )
                return first_half + second_half
            else:
                self._log_warning(
                    f"區間已達最小值仍有 {len(fills)} 筆，可能有遺漏",
                    wallet=wallet_address[:10] + "...",
                )

        return fills

    async def fetch_trades(
        self,
        wallet_address: str,
        start_time: datetime,
        end_time: datetime,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        抓取指定時間範圍內的交易紀錄

        使用時間分段方式抓取，若單一區間回傳量達上限則自動切分重試

        Args:
            wallet_address: 錢包地址
            start_time: 開始時間
            end_time: 結束時間

        Returns:
            交易紀錄列表 (已轉換為統一格式)
        """
        all_trades: List[Dict[str, Any]] = []
        current_start = start_time

        self._log_info(
            f"開始抓取交易",
            wallet=wallet_address[:10] + "...",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
        )

        while current_start < end_time:
            current_end = min(
                current_start + timedelta(days=FETCH_INTERVAL_DAYS),
                end_time,
            )

            fills = await self._fetch_fills_for_interval(
                wallet_address, current_start, current_end
            )

            if fills:
                for fill in fills:
                    trade = HyperliquidTrade.from_api_response(
                        wallet_address, fill
                    )
                    all_trades.append(trade.to_dict())

                self._log_info(
                    f"抓取成功",
                    wallet=wallet_address[:10] + "...",
                    period=f"{current_start.date()} ~ {current_end.date()}",
                    count=len(fills),
                )

            current_start = current_end
            await asyncio.sleep(0.2)

        self._log_info(
            f"完成抓取",
            wallet=wallet_address[:10] + "...",
            total=len(all_trades),
        )

        return all_trades

    async def fetch_all_historical(
        self,
        wallet_address: str,
        since: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        抓取所有歷史交易紀錄

        Args:
            wallet_address: 錢包地址
            since: 從何時開始抓取 (預設 2025-01-01)

        Returns:
            交易紀錄列表
        """
        if since is None:
            since = datetime(2025, 1, 1, tzinfo=timezone.utc)

        return await self.fetch_trades(
            wallet_address=wallet_address,
            start_time=since,
            end_time=datetime.now(timezone.utc),
        )

    async def close(self):
        """關閉連接"""
        # hyperliquid SDK 沒有需要關閉的資源
        self._log_info("Hyperliquid fetcher closed")
