"""Tạo dữ liệu nguồn SQL — chuyển đổi CSV Olist thành 4 file CSV chuẩn.

Đầu vào : sample_data/olist_*.csv   (dữ liệu Olist gốc)
Đầu ra  : data_source/sql/users.csv, products.csv, orders.csv, order_items.csv

Các file đầu ra đã được map lại cột + bổ sung các trường mà pipeline yêu cầu
(full_name, email, phone_number, loyalty_tier, cost_price, total_amount, ...).

Usage:
    python -m generate_fake_data.generate_sql_source [--seed 42] [--sample-frac 1.0]
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("generate_sql")

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SAMPLE_DIR = _PROJECT_ROOT / "sample_data"
_OUTPUT_DIR = _PROJECT_ROOT / "data_source" / "sql"

# --- Tên và họ tiếng Brazil (phổ biến) để sinh full_name ---
FIRST_NAMES = [
    "João", "Maria", "Pedro", "Ana", "Lucas", "Juliana", "Carlos", "Fernanda",
    "Rafael", "Larissa", "Bruno", "Patricia", "Felipe", "Camila", "Matheus",
    "Bianca", "Gustavo", "Amanda", "Thiago", "Letícia", "Leonardo", "Mariana",
    "Gabriel", "Beatriz", "André", "Raquel", "Diego", "Daniela", "Eduardo",
    "Vanessa", "Rodrigo", "Carolina", "Marcelo", "Aline", "Vinícius", "Natália",
]
LAST_NAMES = [
    "Silva", "Santos", "Oliveira", "Souza", "Lima", "Pereira", "Ferreira",
    "Costa", "Rodrigues", "Almeida", "Nascimento", "Araújo", "Melo", "Ribeiro",
    "Barbosa", "Cardoso", "Gomes", "Rocha", "Carvalho", "Martins", "Correia",
    "Mendes", "Moreira", "Freitas", "Nunes", "Reis", "Monteiro", "Teixeira",
]
EMAIL_DOMAINS = ["gmail.com", "hotmail.com", "yahoo.com.br", "outlook.com", "uol.com.br"]

STATUS_MAP = {
    "created": "Pending",
    "approved": "Processing",
    "processing": "Processing",
    "invoiced": "Processing",
    "shipped": "Processing",
    "delivered": "Completed",
    "canceled": "Cancelled",
    "unavailable": "Cancelled",
}
PAYMENT_MAP = {
    "credit_card": "Credit Card",
    "boleto": "Bank Slip",
    "voucher": "Voucher",
    "debit_card": "Debit Card",
}
LOYALTY_TIERS = ["Bronze", "Silver", "Gold", "Platinum"]
LOYALTY_WEIGHTS = [0.50, 0.25, 0.15, 0.10]  # phân bố thực tế


def _read_csv(filename: str) -> list[dict]:
    """Đọc CSV từ sample_data/."""
    path = _SAMPLE_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {path}")
    with open(path, "r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def _write_csv(filename: str, rows: list[dict], fieldnames: list[str]) -> Path:
    """Ghi CSV ra data_source/sql/."""
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    path = _OUTPUT_DIR / filename
    with open(path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("  Đã ghi %d dòng → %s", len(rows), path)
    return path


def _deterministic_name(customer_id: str, rng: random.Random) -> str:
    """Tạo full_name dựa trên hash của customer_id để có kết quả ổn định."""
    h = int(hashlib.md5(customer_id.encode()).hexdigest(), 16)
    first = FIRST_NAMES[h % len(FIRST_NAMES)]
    last = LAST_NAMES[(h >> 8) % len(LAST_NAMES)]
    return f"{first} {last}"


def _deterministic_email(full_name: str, customer_id: str) -> str:
    """Tạo email dựa trên full_name."""
    name_part = full_name.lower().replace(" ", ".").replace("í", "i").replace("á", "a") \
                         .replace("ã", "a").replace("ú", "u").replace("ó", "o") \
                         .replace("ê", "e").replace("é", "e")
    h = int(hashlib.md5(customer_id.encode()).hexdigest()[:8], 16)
    domain = EMAIL_DOMAINS[h % len(EMAIL_DOMAINS)]
    suffix = customer_id[:4]
    return f"{name_part}.{suffix}@{domain}"


def _deterministic_phone(customer_id: str) -> str:
    """Tạo số điện thoại Brazil giả."""
    h = int(hashlib.md5(customer_id.encode()).hexdigest()[:12], 16)
    ddd = 11 + (h % 79)                     # DDD: 11–89
    number = 900000000 + (h % 100000000)     # 9XXXXXXXX
    return f"+55 {ddd} {number}"


# =========================================================
# 1. USERS — từ olist_customers_dataset.csv
# =========================================================
def generate_users(rng: random.Random, sample_frac: float = 1.0) -> list[dict]:
    """Tạo users.csv: user_id, full_name, email, phone_number, customer_city,
    customer_state, loyalty_tier, created_at.
    """
    logger.info("=== Tạo users.csv ===")
    raw = _read_csv("olist_customers_dataset.csv")

    # Lấy danh sách customers unique theo customer_unique_id
    seen: dict[str, dict] = {}
    cid_to_uid: dict[str, str] = {}
    for row in raw:
        uid = row["customer_unique_id"]
        cid_to_uid[row["customer_id"]] = uid
        if uid not in seen:
            seen[uid] = row

    customers = list(seen.values())
    logger.info("  Customers unique: %d", len(customers))

    # --- DERIVED: created_at = MIN(order_purchase_timestamp) per customer ---
    raw_orders = _read_csv("olist_orders_dataset.csv")
    min_timestamp: dict[str, str] = {}
    order_to_uid: dict[str, str] = {}
    for orow in raw_orders:
        cid = orow.get("customer_id", "")
        uid = cid_to_uid.get(cid, "")
        if not uid:
            continue
        order_to_uid[orow["order_id"]] = uid
        ts = orow.get("order_purchase_timestamp", "")
        if ts and (uid not in min_timestamp or ts < min_timestamp[uid]):
            min_timestamp[uid] = ts

    # --- DERIVED: loyalty_tier = percentile dựa trên SUM(payment_value) ---
    raw_payments = _read_csv("olist_order_payments_dataset.csv")
    total_spend: dict[str, float] = {}
    for prow in raw_payments:
        oid = prow.get("order_id", "")
        uid = order_to_uid.get(oid, "")
        if not uid:
            continue
        try:
            val = float(prow.get("payment_value", "0"))
        except (ValueError, TypeError):
            continue
        total_spend[uid] = total_spend.get(uid, 0.0) + val

    # Tính ngưỡng percentile (ascending sort)
    sorted_spends = sorted(total_spend.values())
    n_spenders = len(sorted_spends)
    if n_spenders > 0:
        platinum_threshold = sorted_spends[int(n_spenders * 0.95)]
        gold_threshold = sorted_spends[int(n_spenders * 0.85)]
        silver_threshold = sorted_spends[int(n_spenders * 0.65)]
    else:
        platinum_threshold = gold_threshold = silver_threshold = float("inf")
    logger.info("  Loyalty thresholds: Platinum>=%.2f, Gold>=%.2f, Silver>=%.2f",
                platinum_threshold, gold_threshold, silver_threshold)

    if sample_frac < 1.0:
        k = max(1, int(len(customers) * sample_frac))
        customers = rng.sample(customers, k)

    users = []
    for row in customers:
        uid = row["customer_unique_id"]
        full_name = _deterministic_name(uid, rng)
        email = _deterministic_email(full_name, uid)
        phone = _deterministic_phone(uid)

        # created_at: MIN(order_purchase_timestamp) hoặc fallback hash-based
        if uid in min_timestamp:
            created_at = min_timestamp[uid]
        else:
            h = int(hashlib.md5(uid.encode()).hexdigest()[:8], 16)
            days_offset = h % 540
            created_at = (datetime(2017, 1, 1) + timedelta(days=days_offset)).strftime("%Y-%m-%d %H:%M:%S")

        # loyalty_tier: percentile-based trên tổng chi tiêu thực
        spend = total_spend.get(uid, 0.0)
        if spend <= 0:
            tier = ""  # NULL — khách chưa hoàn thành đơn nào
        elif spend >= platinum_threshold:
            tier = "Platinum"
        elif spend >= gold_threshold:
            tier = "Gold"
        elif spend >= silver_threshold:
            tier = "Silver"
        else:
            tier = "Bronze"

        users.append({
            "user_id": uid,
            "full_name": full_name,
            "email": email if rng.random() > 0.10 else "",  # ~10% NULL
            "phone_number": phone if rng.random() > 0.15 else "",  # ~15% NULL
            "customer_city": row.get("customer_city", ""),
            "customer_state": row.get("customer_state", ""),
            "loyalty_tier": tier,
            "created_at": created_at,
        })

    _write_csv("users.csv", users, [
        "user_id", "full_name", "email", "phone_number",
        "customer_city", "customer_state", "loyalty_tier", "created_at",
    ])
    return users


# =========================================================
# 2. PRODUCTS — từ olist_products_dataset.csv
# =========================================================
def generate_products(rng: random.Random, order_items: list[dict] | None = None,
                      sample_frac: float = 1.0) -> list[dict]:
    """Tạo products.csv: product_id, product_name, category, cost_price."""
    logger.info("=== Tạo products.csv ===")
    raw = _read_csv("olist_products_dataset.csv")
    translations = _load_category_translations()

    if sample_frac < 1.0:
        k = max(1, int(len(raw) * sample_frac))
        raw = rng.sample(raw, k)

    # Tính avg price từ order_items nếu có
    avg_prices: dict[str, float] = {}
    if order_items:
        sums: dict[str, list[float]] = {}
        for item in order_items:
            pid = item.get("product_id", "")
            price_str = item.get("price", item.get("unit_price", "0"))
            try:
                price = float(price_str)
            except (ValueError, TypeError):
                continue
            sums.setdefault(pid, []).append(price)
        avg_prices = {pid: sum(vals) / len(vals) for pid, vals in sums.items()}

    products = []
    for row in raw:
        pid = row["product_id"]
        cat_raw = row.get("product_category_name", "") or ""
        cat_translated = translations.get(cat_raw, cat_raw).replace("_", " ").title() if cat_raw else ""

        # product_name: tạo từ category + hash
        h = int(hashlib.md5(pid.encode()).hexdigest()[:6], 16)
        name = f"{cat_translated} Item #{h % 10000}" if cat_translated else f"Product #{h % 10000}"

        # cost_price: 70% giá bán trung bình hoặc random
        if pid in avg_prices:
            cost_price = round(avg_prices[pid] * 0.7, 2)
        else:
            cost_price = round(rng.uniform(10.0, 500.0), 2)

        products.append({
            "product_id": pid,
            "product_name": name,
            "category": cat_translated if cat_translated else "",
            "cost_price": cost_price,
        })

    _write_csv("products.csv", products, [
        "product_id", "product_name", "category", "cost_price",
    ])
    return products


def _load_category_translations() -> dict[str, str]:
    """Đọc bảng dịch category tiếng Bồ Đào Nha → tiếng Anh."""
    path = _SAMPLE_DIR / "product_category_name_translation.csv"
    if not path.exists():
        return {}
    mapping: dict[str, str] = {}
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            pt = row.get("product_category_name", "")
            en = row.get("product_category_name_english", "")
            if pt and en:
                mapping[pt] = en
    return mapping


# =========================================================
# 3. ORDERS — từ olist_orders_dataset.csv + olist_order_payments_dataset.csv
# =========================================================
def generate_orders(rng: random.Random, user_ids: set[str],
                    order_items_raw: list[dict],
                    sample_frac: float = 1.0) -> tuple[list[dict], dict[str, str]]:
    """Tạo orders.csv: order_id, user_id, total_amount, order_status,
    payment_method, created_at.

    Trả về (orders, order_to_user_map) để dùng cho order_items.
    """
    logger.info("=== Tạo orders.csv ===")
    raw_orders = _read_csv("olist_orders_dataset.csv")
    raw_payments = _read_csv("olist_order_payments_dataset.csv")

    # Map order → primary payment method (payment_sequential = 1)
    payment_by_order: dict[str, str] = {}
    for row in raw_payments:
        oid = row["order_id"]
        seq = row.get("payment_sequential", "")
        if seq == "1":
            raw_method = row.get("payment_type", "")
            payment_by_order[oid] = PAYMENT_MAP.get(raw_method, "Credit Card")

    # Map order → total amount = SUM(payment_value) per order_id
    total_by_order: dict[str, float] = {}
    for row in raw_payments:
        oid = row.get("order_id", "")
        try:
            val = float(row.get("payment_value", "0"))
        except (ValueError, TypeError):
            continue
        total_by_order[oid] = total_by_order.get(oid, 0.0) + val

    # Map customer_id → user_id (customer_unique_id)
    raw_customers = _read_csv("olist_customers_dataset.csv")
    cid_to_uid: dict[str, str] = {}
    for row in raw_customers:
        cid_to_uid[row["customer_id"]] = row["customer_unique_id"]

    if sample_frac < 1.0:
        k = max(1, int(len(raw_orders) * sample_frac))
        raw_orders = rng.sample(raw_orders, k)

    orders = []
    order_to_user: dict[str, str] = {}
    for row in raw_orders:
        oid = row["order_id"]
        cid = row.get("customer_id", "")
        uid = cid_to_uid.get(cid, cid)

        status_raw = row.get("order_status", "")
        status = STATUS_MAP.get(status_raw, "Pending")

        # created_at: dùng order_purchase_timestamp
        created_at = row.get("order_purchase_timestamp", "")

        total = total_by_order.get(oid, round(rng.uniform(20.0, 500.0), 2))
        payment = payment_by_order.get(oid, "Credit Card")

        orders.append({
            "order_id": oid,
            "user_id": uid,
            "total_amount": round(total, 2),
            "order_status": status,
            "payment_method": payment,
            "created_at": created_at,
        })
        order_to_user[oid] = uid

    _write_csv("orders.csv", orders, [
        "order_id", "user_id", "total_amount", "order_status",
        "payment_method", "created_at",
    ])
    return orders, order_to_user


# =========================================================
# 4. ORDER ITEMS — từ olist_order_items_dataset.csv
# =========================================================
def generate_order_items(rng: random.Random, order_items_raw: list[dict],
                         sample_frac: float = 1.0) -> list[dict]:
    """Tạo order_items.csv: item_id, order_id, product_id, quantity, unit_price."""
    logger.info("=== Tạo order_items.csv ===")

    if sample_frac < 1.0:
        k = max(1, int(len(order_items_raw) * sample_frac))
        order_items_raw = rng.sample(order_items_raw, k)

    items = []
    for row in order_items_raw:
        oid = row.get("order_id", "")
        seq = row.get("order_item_id", "1")
        pid = row.get("product_id", "")
        price_str = row.get("price", "0")
        try:
            price = float(price_str)
        except (ValueError, TypeError):
            price = 0.0

        items.append({
            "item_id": f"{oid}_{seq}",
            "order_id": oid,
            "product_id": pid,
            "quantity": 1,
            "unit_price": round(price, 2),
        })

    _write_csv("order_items.csv", items, [
        "item_id", "order_id", "product_id", "quantity", "unit_price",
    ])
    return items


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tạo dữ liệu nguồn SQL (CSV) từ sample_data Olist"
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed (mặc định: 42)")
    parser.add_argument("--sample-frac", type=float, default=1.0,
                        help="Tỉ lệ lấy mẫu (0.0–1.0), mặc định 1.0 = toàn bộ")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    logger.info("Bắt đầu tạo dữ liệu SQL (seed=%d, sample=%.0f%%)",
                args.seed, args.sample_frac * 100)

    # Đọc order_items trước vì nhiều bảng cần dùng
    order_items_raw = _read_csv("olist_order_items_dataset.csv")

    # 1. Users
    users = generate_users(rng, args.sample_frac)
    user_ids = {u["user_id"] for u in users}

    # 2. Products
    generate_products(rng, order_items_raw, args.sample_frac)

    # 3. Orders
    generate_orders(rng, user_ids, order_items_raw, args.sample_frac)

    # 4. Order Items
    generate_order_items(rng, order_items_raw, args.sample_frac)

    logger.info("✅ Hoàn tất! File đầu ra tại: %s", _OUTPUT_DIR)


if __name__ == "__main__":
    main()
