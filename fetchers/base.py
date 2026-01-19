"""
Base fetcher class for trade data.
"""

import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)


class BaseFetcher(ABC):
    """交易資料抓取器基礎類別"""

    platform_name: str = "base"

    @abstractmethod
    async def fetch_trades(
        self,
        wallet_address: str,
        start_time: datetime,
        end_time: datetime,
        **kwargs,
    ) -> List[Dict[str, Any]]:
        """
        抓取指定時間範圍內的交易紀錄

        Args:
            wallet_address: 錢包地址
            start_time: 開始時間
            end_time: 結束時間
            **kwargs: 額外參數 (如 API credentials)

        Returns:
            交易紀錄列表 (已轉換為統一格式)
        """
        pass

    @abstractmethod
    async def close(self):
        """關閉連接"""
        pass

    def _log_info(self, message: str, **kwargs):
        """記錄資訊日誌"""
        extra = " ".join(f"{k}={v}" for k, v in kwargs.items())
        logger.info(f"[{self.platform_name}] {message} {extra}".strip())

    def _log_error(self, message: str, **kwargs):
        """記錄錯誤日誌"""
        extra = " ".join(f"{k}={v}" for k, v in kwargs.items())
        logger.error(f"[{self.platform_name}] {message} {extra}".strip())

    def _log_warning(self, message: str, **kwargs):
        """記錄警告日誌"""
        extra = " ".join(f"{k}={v}" for k, v in kwargs.items())
        logger.warning(f"[{self.platform_name}] {message} {extra}".strip())
