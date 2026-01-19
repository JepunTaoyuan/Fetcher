#!/usr/bin/env python3
"""
MongoDB Users Collection 讀取工具

獨立腳本，用於讀取和查詢 referral_system 資料庫中的 users collection。
此腳本與主專案隔離，可獨立運行。

使用方式:
    python read_users.py                    # 互動式選單
    python read_users.py --all              # 列出所有用戶
    python read_users.py --id USER_ID       # 依 ID 查詢
    python read_users.py --wallet ADDRESS   # 依錢包地址查詢
    python read_users.py --affiliates       # 列出所有 affiliates
    python read_users.py --referrals AFF_ID # 查詢特定 affiliate 的下線

環境變數:
    MONGODB_URI     - MongoDB 連線 URI (預設: mongodb://localhost:27017)
    DATABASE_NAME   - 資料庫名稱 (預設: referral_system)
"""

import asyncio
import argparse
import os
import sys
from datetime import datetime
from typing import Optional, List, Dict, Any

from motor.motor_asyncio import AsyncIOMotorClient
from dotenv import load_dotenv

# 載入 .env 文件
load_dotenv()


class UsersReader:
    """MongoDB Users Collection 讀取器"""

    def __init__(self, mongodb_uri: str, database_name: str):
        self.mongodb_uri = mongodb_uri
        self.database_name = database_name
        self.client: Optional[AsyncIOMotorClient] = None
        self.db = None

    async def connect(self):
        """建立資料庫連接"""
        try:
            self.client = AsyncIOMotorClient(self.mongodb_uri)
            self.db = self.client[self.database_name]
            # 測試連接
            await self.client.admin.command('ping')
            print(f"✓ 成功連接到 MongoDB: {self.database_name}")
        except Exception as e:
            print(f"✗ 連接 MongoDB 失敗: {e}")
            raise

    async def disconnect(self):
        """關閉資料庫連接"""
        if self.client:
            self.client.close()
            print("✓ 已關閉資料庫連接")

    async def get_all_users(self, limit: int = 100) -> List[Dict[str, Any]]:
        """
        取得所有用戶

        Args:
            limit: 最大回傳數量，預設 100

        Returns:
            用戶列表
        """
        cursor = self.db.users.find().limit(limit)
        users = await cursor.to_list(length=limit)
        return users

    async def get_user_by_id(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        依 ID 取得用戶

        Args:
            user_id: 用戶 ID

        Returns:
            用戶資料或 None
        """
        user = await self.db.users.find_one({"_id": user_id})
        return user

    async def get_user_by_wallet(self, wallet_address: str) -> Optional[Dict[str, Any]]:
        """
        依錢包地址取得用戶

        Args:
            wallet_address: 錢包地址

        Returns:
            用戶資料或 None
        """
        user = await self.db.users.find_one({"wallet_address": wallet_address})
        return user

    async def get_affiliates(self) -> List[Dict[str, Any]]:
        """
        取得所有 affiliates（推廣者）

        Returns:
            affiliate 用戶列表
        """
        cursor = self.db.users.find({"is_affiliate": True})
        affiliates = await cursor.to_list(length=1000)
        return affiliates

    async def get_referrals_by_affiliate(self, affiliate_id: str) -> List[Dict[str, Any]]:
        """
        取得特定 affiliate 的所有下線

        Args:
            affiliate_id: 推廣者 ID

        Returns:
            下線用戶列表
        """
        cursor = self.db.users.find({"parent_affiliate_id": affiliate_id})
        referrals = await cursor.to_list(length=1000)
        return referrals

    async def get_users_count(self) -> int:
        """取得用戶總數"""
        count = await self.db.users.count_documents({})
        return count

    async def get_affiliates_count(self) -> int:
        """取得 affiliate 總數"""
        count = await self.db.users.count_documents({"is_affiliate": True})
        return count


def format_timestamp(timestamp: Optional[int]) -> str:
    """格式化時間戳"""
    if timestamp is None:
        return "N/A"
    try:
        dt = datetime.fromtimestamp(timestamp)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    except (ValueError, OSError):
        return str(timestamp)


def format_user(user: Dict[str, Any]) -> str:
    """格式化用戶資料為可讀字串"""
    lines = [
        "=" * 60,
        f"用戶 ID:           {user.get('_id', 'N/A')}",
        f"錢包地址:          {user.get('wallet_address', 'N/A')}",
        f"API Key:           {user.get('user_api_key', 'N/A')[:20] + '...' if user.get('user_api_key') else 'N/A'}",
        f"使用的邀請碼:      {user.get('used_referral_code', 'N/A')}",
        f"是否為推廣者:      {'是' if user.get('is_affiliate') else '否'}",
        f"上級推廣者 ID:     {user.get('parent_affiliate_id', 'N/A')}",
        f"最大返佣率:        {user.get('max_referral_rate', 0) * 100:.1f}%",
        f"手續費折扣率:      {user.get('fee_discount_rate', 0) * 100:.1f}%",
        f"是否為管理員:      {'是' if user.get('is_admin') else '否'}",
        f"總交易量:          {user.get('total_volume', 0):,.2f}",
        f"週交易量:          {user.get('weekly_volume', 0):,.2f}",
        f"建立時間:          {format_timestamp(user.get('created_at'))}",
        "=" * 60,
    ]
    return "\n".join(lines)


def format_user_summary(user: Dict[str, Any]) -> str:
    """格式化用戶摘要（單行）"""
    user_id = user.get('_id', 'N/A')[:20]
    wallet = user.get('wallet_address', 'N/A')[:15] + '...' if user.get('wallet_address') else 'N/A'
    is_affiliate = "推廣者" if user.get('is_affiliate') else "一般"
    volume = user.get('total_volume', 0)
    return f"{user_id:<20} | {wallet:<18} | {is_affiliate:<6} | 交易量: {volume:>12,.2f}"


async def interactive_menu(reader: UsersReader):
    """互動式選單"""
    while True:
        print("\n" + "=" * 60)
        print("MongoDB Users 查詢工具")
        print("=" * 60)

        # 顯示統計資訊
        total_users = await reader.get_users_count()
        total_affiliates = await reader.get_affiliates_count()
        print(f"總用戶數: {total_users} | 推廣者數: {total_affiliates}")

        print("-" * 60)
        print("1. 列出所有用戶")
        print("2. 依 ID 查詢用戶")
        print("3. 依錢包地址查詢用戶")
        print("4. 列出所有推廣者 (Affiliates)")
        print("5. 查詢特定推廣者的下線")
        print("0. 退出")
        print("-" * 60)

        choice = input("請選擇操作 (0-5): ").strip()

        if choice == "0":
            print("再見！")
            break
        elif choice == "1":
            await handle_list_all_users(reader)
        elif choice == "2":
            await handle_query_by_id(reader)
        elif choice == "3":
            await handle_query_by_wallet(reader)
        elif choice == "4":
            await handle_list_affiliates(reader)
        elif choice == "5":
            await handle_query_referrals(reader)
        else:
            print("無效的選擇，請重試")


async def handle_list_all_users(reader: UsersReader):
    """處理列出所有用戶"""
    limit_input = input("請輸入要顯示的數量 (預設 20): ").strip()
    limit = int(limit_input) if limit_input.isdigit() else 20

    users = await reader.get_all_users(limit=limit)

    if not users:
        print("\n找不到任何用戶")
        return

    print(f"\n找到 {len(users)} 位用戶:")
    print("-" * 80)
    print(f"{'ID':<20} | {'錢包地址':<18} | {'類型':<6} | {'交易量':>15}")
    print("-" * 80)

    for user in users:
        print(format_user_summary(user))


async def handle_query_by_id(reader: UsersReader):
    """處理依 ID 查詢"""
    user_id = input("請輸入用戶 ID: ").strip()

    if not user_id:
        print("用戶 ID 不能為空")
        return

    user = await reader.get_user_by_id(user_id)

    if user:
        print("\n" + format_user(user))
    else:
        print(f"\n找不到用戶: {user_id}")


async def handle_query_by_wallet(reader: UsersReader):
    """處理依錢包地址查詢"""
    wallet = input("請輸入錢包地址: ").strip()

    if not wallet:
        print("錢包地址不能為空")
        return

    user = await reader.get_user_by_wallet(wallet)

    if user:
        print("\n" + format_user(user))
    else:
        print(f"\n找不到錢包地址: {wallet}")


async def handle_list_affiliates(reader: UsersReader):
    """處理列出所有 affiliates"""
    affiliates = await reader.get_affiliates()

    if not affiliates:
        print("\n找不到任何推廣者")
        return

    print(f"\n找到 {len(affiliates)} 位推廣者:")
    print("-" * 80)
    print(f"{'ID':<20} | {'錢包地址':<18} | {'最大返佣率':>10} | {'交易量':>15}")
    print("-" * 80)

    for aff in affiliates:
        user_id = aff.get('_id', 'N/A')[:20]
        wallet = aff.get('wallet_address', 'N/A')[:15] + '...' if aff.get('wallet_address') else 'N/A'
        rate = aff.get('max_referral_rate', 0) * 100
        volume = aff.get('total_volume', 0)
        print(f"{user_id:<20} | {wallet:<18} | {rate:>9.1f}% | {volume:>15,.2f}")


async def handle_query_referrals(reader: UsersReader):
    """處理查詢特定 affiliate 的下線"""
    affiliate_id = input("請輸入推廣者 ID: ").strip()

    if not affiliate_id:
        print("推廣者 ID 不能為空")
        return

    # 先確認該 affiliate 存在
    affiliate = await reader.get_user_by_id(affiliate_id)
    if not affiliate:
        print(f"\n找不到推廣者: {affiliate_id}")
        return

    if not affiliate.get('is_affiliate'):
        print(f"\n用戶 {affiliate_id} 不是推廣者")
        return

    referrals = await reader.get_referrals_by_affiliate(affiliate_id)

    if not referrals:
        print(f"\n推廣者 {affiliate_id} 沒有下線用戶")
        return

    print(f"\n推廣者 {affiliate_id} 的下線用戶 ({len(referrals)} 人):")
    print("-" * 80)
    print(f"{'ID':<20} | {'錢包地址':<18} | {'手續費折扣':>10} | {'交易量':>15}")
    print("-" * 80)

    for ref in referrals:
        user_id = ref.get('_id', 'N/A')[:20]
        wallet = ref.get('wallet_address', 'N/A')[:15] + '...' if ref.get('wallet_address') else 'N/A'
        discount = ref.get('fee_discount_rate', 0) * 100
        volume = ref.get('total_volume', 0)
        print(f"{user_id:<20} | {wallet:<18} | {discount:>9.1f}% | {volume:>15,.2f}")


async def run_cli(args: argparse.Namespace, reader: UsersReader):
    """執行命令列模式"""
    if args.all:
        users = await reader.get_all_users(limit=args.limit)
        if not users:
            print("找不到任何用戶")
            return
        print(f"找到 {len(users)} 位用戶:\n")
        for user in users:
            print(format_user(user))

    elif args.id:
        user = await reader.get_user_by_id(args.id)
        if user:
            print(format_user(user))
        else:
            print(f"找不到用戶: {args.id}")

    elif args.wallet:
        user = await reader.get_user_by_wallet(args.wallet)
        if user:
            print(format_user(user))
        else:
            print(f"找不到錢包地址: {args.wallet}")

    elif args.affiliates:
        affiliates = await reader.get_affiliates()
        if not affiliates:
            print("找不到任何推廣者")
            return
        print(f"找到 {len(affiliates)} 位推廣者:\n")
        for aff in affiliates:
            print(format_user(aff))

    elif args.referrals:
        referrals = await reader.get_referrals_by_affiliate(args.referrals)
        if not referrals:
            print(f"推廣者 {args.referrals} 沒有下線用戶")
            return
        print(f"推廣者 {args.referrals} 的下線用戶 ({len(referrals)} 人):\n")
        for ref in referrals:
            print(format_user(ref))


def parse_args() -> argparse.Namespace:
    """解析命令列參數"""
    parser = argparse.ArgumentParser(
        description="MongoDB Users Collection 讀取工具",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
範例:
    python read_users.py                    # 互動式選單
    python read_users.py --all              # 列出所有用戶
    python read_users.py --all --limit 50   # 列出前 50 位用戶
    python read_users.py --id user123       # 依 ID 查詢
    python read_users.py --wallet 0x742...  # 依錢包地址查詢
    python read_users.py --affiliates       # 列出所有推廣者
    python read_users.py --referrals aff123 # 查詢推廣者的下線
        """
    )

    parser.add_argument(
        "--all", "-a",
        action="store_true",
        help="列出所有用戶"
    )
    parser.add_argument(
        "--id", "-i",
        type=str,
        help="依 ID 查詢用戶"
    )
    parser.add_argument(
        "--wallet", "-w",
        type=str,
        help="依錢包地址查詢用戶"
    )
    parser.add_argument(
        "--affiliates",
        action="store_true",
        help="列出所有推廣者"
    )
    parser.add_argument(
        "--referrals", "-r",
        type=str,
        help="查詢特定推廣者的下線"
    )
    parser.add_argument(
        "--limit", "-l",
        type=int,
        default=100,
        help="最大回傳數量 (預設: 100)"
    )
    parser.add_argument(
        "--mongodb-uri",
        type=str,
        default=os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
        help="MongoDB 連線 URI (預設使用環境變數 MONGODB_URI)"
    )
    parser.add_argument(
        "--database",
        type=str,
        default=os.getenv("DATABASE_NAME", "referral_system"),
        help="資料庫名稱 (預設使用環境變數 DATABASE_NAME)"
    )

    return parser.parse_args()


async def main():
    """主程式入口"""
    args = parse_args()

    # 建立讀取器
    reader = UsersReader(
        mongodb_uri=args.mongodb_uri,
        database_name=args.database
    )

    try:
        # 連接資料庫
        await reader.connect()

        # 判斷是互動模式還是命令列模式
        has_cli_args = any([args.all, args.id, args.wallet, args.affiliates, args.referrals])

        if has_cli_args:
            await run_cli(args, reader)
        else:
            await interactive_menu(reader)

    except KeyboardInterrupt:
        print("\n\n操作已取消")
    except Exception as e:
        print(f"\n錯誤: {e}")
        sys.exit(1)
    finally:
        await reader.disconnect()


if __name__ == "__main__":
    asyncio.run(main())
