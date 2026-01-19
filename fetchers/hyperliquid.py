"""
Hyperliquid 交易資料抓取器
"""

import asyncio
import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from hyperliquid.info import Info

from .base import BaseFetcher
from ..models.trade import HyperliquidTrade

logger = logging.getLogger(__name__)

# 每次抓取的時間區間 (天)
FETCH_INTERVAL_DAYS = 30


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

    async def fetch_trades(
        self,
        wallet_address: str,
        start_time: datetime,
        end_time: datetime,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        抓取指定時間範圍內的交易紀錄

        使用時間分段方式抓取，避免單次請求返回過多資料

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

            # 轉換為毫秒時間戳
            start_ts = int(current_start.timestamp() * 1000)
            end_ts = int(current_end.timestamp() * 1000)

            try:
                # 使用同步 API (hyperliquid-python-sdk 是同步的)
                # 在 async 環境中使用 run_in_executor
                loop = asyncio.get_event_loop()
                fills = await loop.run_in_executor(
                    None,
                    lambda: self.info.user_fills_by_time(
                        wallet_address, start_ts, end_ts
                    ),
                )

                if fills:
                    # 轉換為統一格式
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

                    # 如果返回的資料量很大，可能還有更多，縮短時間區間
                    if len(fills) >= 500:
                        self._log_warning(
                            f"返回資料量大，可能有遺漏",
                            count=len(fills),
                        )

                # 移動到下一個時間區間
                current_start = current_end

                # Rate limit 保護
                await asyncio.sleep(0.2)

            except Exception as e:
                self._log_error(
                    f"抓取失敗",
                    wallet=wallet_address[:10] + "...",
                    period=f"{current_start.date()} ~ {current_end.date()}",
                    error=str(e),
                )
                # 發生錯誤時仍然繼續下一個時間區間
                current_start = current_end
                await asyncio.sleep(1)

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
            since = datetime(2025, 1, 1)

        return await self.fetch_trades(
            wallet_address=wallet_address,
            start_time=since,
            end_time=datetime.now(),
        )

    async def close(self):
        """關閉連接"""
        # hyperliquid SDK 沒有需要關閉的資源
        self._log_info("Hyperliquid fetcher closed")
