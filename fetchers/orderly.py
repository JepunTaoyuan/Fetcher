"""
Orderly 交易資料抓取器
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from orderly_evm_connector.rest import Rest

from .base import BaseFetcher
from models.trade import OrderlyTrade

logger = logging.getLogger(__name__)

# 每頁抓取數量 (Orderly 最大 500)
PAGE_SIZE = 500


class OrderlyFetcher(BaseFetcher):
    """Orderly 交易資料抓取器"""

    platform_name = "orderly"

    def __init__(self):
        """初始化 Orderly 抓取器"""
        self._clients: Dict[str, Rest] = {}
        self._log_info("Orderly fetcher initialized")

    def _get_client(
        self,
        orderly_key: str,
        orderly_secret: str,
        account_id: str,
    ) -> Rest:
        """
        取得或建立 REST client

        Args:
            orderly_key: Orderly API key
            orderly_secret: Orderly API secret
            account_id: Orderly account ID

        Returns:
            Rest client instance
        """
        cache_key = f"{account_id}"

        if cache_key not in self._clients:
            self._clients[cache_key] = Rest(
                orderly_key=orderly_key,
                orderly_secret=orderly_secret,
                orderly_account_id=account_id,
            )

        return self._clients[cache_key]

    async def fetch_trades(
        self,
        wallet_address: str,
        start_time: datetime,
        end_time: datetime,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        抓取指定時間範圍內的交易紀錄

        使用分頁方式抓取所有資料

        Args:
            wallet_address: 錢包地址
            start_time: 開始時間
            end_time: 結束時間
            **kwargs: 額外參數
                - orderly_key: API key
                - orderly_secret: API secret
                - account_id: Account ID

        Returns:
            交易紀錄列表 (已轉換為統一格式)
        """
        orderly_key = kwargs.get("orderly_key")
        orderly_secret = kwargs.get("orderly_secret")
        account_id = kwargs.get("account_id")

        if not all([orderly_key, orderly_secret, account_id]):
            self._log_error(
                "缺少 Orderly API 憑證",
                wallet=wallet_address[:10] + "...",
            )
            return []

        client = self._get_client(orderly_key, orderly_secret, account_id)

        all_trades: List[Dict[str, Any]] = []
        page = 1

        # 轉換為毫秒時間戳
        start_ts = int(start_time.timestamp() * 1000)
        end_ts = int(end_time.timestamp() * 1000)

        self._log_info(
            f"開始抓取交易",
            wallet=wallet_address[:10] + "...",
            account_id=account_id[:20] + "...",
            start=start_time.isoformat(),
            end=end_time.isoformat(),
        )

        while True:
            try:
                loop = asyncio.get_running_loop()
                response = await loop.run_in_executor(
                    None,
                    lambda: client.get_trades(
                        start_t=start_ts,
                        end_t=end_ts,
                        page=page,
                        size=PAGE_SIZE,
                    ),
                )

                # 解析回應
                # Orderly API 回傳格式可能是:
                # {"success": true, "data": {"rows": [...], "meta": {...}}}
                # 或直接是 {"rows": [...]}
                trades_data = []
                if isinstance(response, dict):
                    if "data" in response:
                        data = response["data"]
                        if isinstance(data, dict) and "rows" in data:
                            trades_data = data["rows"]
                        elif isinstance(data, list):
                            trades_data = data
                    elif "rows" in response:
                        trades_data = response["rows"]
                    elif isinstance(response, list):
                        trades_data = response

                if not trades_data:
                    self._log_info(
                        f"無更多資料",
                        wallet=wallet_address[:10] + "...",
                        page=page,
                    )
                    break

                # 轉換為統一格式
                for trade_data in trades_data:
                    trade = OrderlyTrade.from_api_response(
                        wallet_address, account_id, trade_data
                    )
                    all_trades.append(trade.to_dict())

                self._log_info(
                    f"抓取成功",
                    wallet=wallet_address[:10] + "...",
                    page=page,
                    count=len(trades_data),
                )

                # 如果返回的資料量小於頁面大小，表示已經沒有更多資料
                if len(trades_data) < PAGE_SIZE:
                    break

                page += 1

                # Rate limit 保護
                await asyncio.sleep(0.3)

            except Exception as e:
                self._log_error(
                    f"抓取失敗",
                    wallet=wallet_address[:10] + "...",
                    page=page,
                    error=str(e),
                )
                break

        self._log_info(
            f"完成抓取",
            wallet=wallet_address[:10] + "...",
            total=len(all_trades),
        )

        return all_trades

    async def fetch_all_historical(
        self,
        wallet_address: str,
        orderly_key: str,
        orderly_secret: str,
        account_id: str,
        since: Optional[datetime] = None,
    ) -> List[Dict[str, Any]]:
        """
        抓取所有歷史交易紀錄

        Args:
            wallet_address: 錢包地址
            orderly_key: API key
            orderly_secret: API secret
            account_id: Account ID
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
            orderly_key=orderly_key,
            orderly_secret=orderly_secret,
            account_id=account_id,
        )

    async def close(self):
        """關閉連接"""
        self._clients.clear()
        self._log_info("Orderly fetcher closed")
