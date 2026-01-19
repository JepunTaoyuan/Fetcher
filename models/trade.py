"""
Trade dataclass 定義
"""

from dataclasses import dataclass, asdict
from datetime import datetime
from decimal import Decimal
from typing import Optional, Dict, Any


@dataclass
class HyperliquidTrade:
    """Hyperliquid 交易紀錄"""

    wallet_address: str
    trade_id: int
    symbol: str
    side: str  # "BUY" / "SELL"
    price: Decimal
    quantity: Decimal
    executed_at: datetime

    # Optional fields
    tx_hash: Optional[str] = None
    order_id: Optional[int] = None
    direction: Optional[str] = None  # "LONG" / "SHORT"
    fee: Optional[Decimal] = None
    fee_token: Optional[str] = None
    realized_pnl: Optional[Decimal] = None
    is_taker: Optional[bool] = None
    position_before: Optional[Decimal] = None

    def to_dict(self) -> Dict[str, Any]:
        """轉換為字典"""
        return asdict(self)

    @classmethod
    def from_api_response(
        cls, wallet_address: str, data: Dict[str, Any]
    ) -> "HyperliquidTrade":
        """
        從 Hyperliquid API 回應建立 Trade 物件

        API 回傳結構:
        {
            "coin": str,           # 交易對
            "px": str,             # 成交價格
            "sz": str,             # 成交數量
            "side": str,           # "B"=買, "A"=賣
            "time": int,           # Unix 時間戳 (毫秒)
            "startPosition": str,  # 成交前倉位
            "dir": str,            # "Long", "Short"
            "closedPnl": str,      # 已實現盈虧
            "hash": str,           # 交易 hash
            "oid": int,            # 訂單 ID
            "crossed": bool,       # 是否為 taker
            "fee": str,            # 手續費
            "tid": int,            # 交易 ID (唯一)
            "feeToken": str,       # 手續費幣種
        }
        """
        # 轉換 side: "B" -> "BUY", "A" -> "SELL"
        raw_side = data.get("side", "")
        side = "BUY" if raw_side == "B" else "SELL" if raw_side == "A" else raw_side

        # 轉換 direction: 標準化為大寫
        raw_dir = data.get("dir", "")
        direction = raw_dir.upper() if raw_dir else None

        # 轉換時間戳 (毫秒 -> datetime)
        time_ms = data.get("time", 0)
        executed_at = datetime.fromtimestamp(time_ms / 1000)

        return cls(
            wallet_address=wallet_address,
            trade_id=data.get("tid", 0),
            tx_hash=data.get("hash"),
            symbol=data.get("coin", ""),
            order_id=data.get("oid"),
            side=side,
            direction=direction,
            price=Decimal(str(data.get("px", "0"))),
            quantity=Decimal(str(data.get("sz", "0"))),
            fee=Decimal(str(data.get("fee", "0"))) if data.get("fee") else None,
            fee_token=data.get("feeToken"),
            realized_pnl=Decimal(str(data.get("closedPnl", "0"))) if data.get("closedPnl") else None,
            is_taker=data.get("crossed"),
            position_before=Decimal(str(data.get("startPosition", "0"))) if data.get("startPosition") else None,
            executed_at=executed_at,
        )


@dataclass
class OrderlyTrade:
    """Orderly 交易紀錄"""

    wallet_address: str
    account_id: str
    trade_id: int
    symbol: str
    side: str  # "BUY" / "SELL"
    price: Decimal
    quantity: Decimal
    executed_at: datetime

    # Optional fields
    order_id: Optional[int] = None
    fee: Optional[Decimal] = None
    fee_token: Optional[str] = None
    realized_pnl: Optional[Decimal] = None
    is_taker: Optional[bool] = None

    def to_dict(self) -> Dict[str, Any]:
        """轉換為字典"""
        return asdict(self)

    @classmethod
    def from_api_response(
        cls, wallet_address: str, account_id: str, data: Dict[str, Any]
    ) -> "OrderlyTrade":
        """
        從 Orderly API 回應建立 Trade 物件

        API 回傳結構 (預估):
        {
            "id": int,              # 交易 ID
            "symbol": str,          # 交易對 (如 "PERP_ETH_USDC")
            "order_id": int,        # 訂單 ID
            "side": str,            # "BUY", "SELL"
            "executed_price": float,# 成交價格
            "executed_quantity": float, # 成交數量
            "fee": float,           # 手續費
            "fee_asset": str,       # 手續費幣種
            "realized_pnl": float,  # 已實現盈虧
            "is_maker": bool,       # 是否為 maker
            "created_time": int,    # Unix 時間戳 (毫秒)
            "trade_id": int,        # 交易 ID (唯一)
        }
        """
        # 轉換時間戳 (毫秒 -> datetime)
        time_ms = data.get("created_time") or data.get("timestamp", 0)
        executed_at = datetime.fromtimestamp(time_ms / 1000) if time_ms else datetime.now()

        # 確定交易 ID
        trade_id = data.get("trade_id") or data.get("id", 0)

        # 確定 side
        side = data.get("side", "").upper()

        # is_taker = NOT is_maker
        is_maker = data.get("is_maker")
        is_taker = not is_maker if is_maker is not None else None

        return cls(
            wallet_address=wallet_address,
            account_id=account_id,
            trade_id=trade_id,
            symbol=data.get("symbol", ""),
            order_id=data.get("order_id"),
            side=side,
            price=Decimal(str(data.get("executed_price", 0))),
            quantity=Decimal(str(data.get("executed_quantity", 0))),
            fee=Decimal(str(data.get("fee", 0))) if data.get("fee") is not None else None,
            fee_token=data.get("fee_asset"),
            realized_pnl=Decimal(str(data.get("realized_pnl", 0))) if data.get("realized_pnl") is not None else None,
            is_taker=is_taker,
            executed_at=executed_at,
        )
