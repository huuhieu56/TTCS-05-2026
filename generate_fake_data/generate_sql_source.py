"""Tạo dữ liệu nguồn SQL — sinh init.sql cho PostgreSQL từ Olist sample_data."""

from __future__ import annotations

import argparse
import hashlib
import logging
import random
from datetime import datetime, timedelta
from pathlib import Path

from generate_fake_data.helpers import (
    deterministic_email,
    deterministic_full_name,
    deterministic_phone,
    read_csv as _read_csv_helper,
    shift_timestamp,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("generate_sql")

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_INIT_SQL_PATH = _PROJECT_ROOT / "infra" / "source-db" / "init.sql"

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


def _read_csv(filename: str) -> list[dict]:  # noqa: D401
    return _read_csv_helper(filename)


# =========================================================
# TABLE SCHEMAS (for CREATE TABLE DDL)
# =========================================================
_TABLE_SCHEMAS = {
    "users": [
        ("user_id", "TEXT PRIMARY KEY"),
        ("full_name", "TEXT"),
        ("email", "TEXT"),
        ("phone_number", "TEXT"),
        ("customer_city", "TEXT"),
        ("customer_state", "TEXT"),
        ("loyalty_tier", "TEXT"),
        ("created_at", "TIMESTAMP"),
    ],
    "products": [
        ("product_id", "TEXT PRIMARY KEY"),
        ("product_name", "TEXT"),
        ("category", "TEXT"),
        ("cost_price", "NUMERIC(12,2)"),
    ],
    "orders": [
        ("order_id", "TEXT PRIMARY KEY"),
        ("user_id", "TEXT"),
        ("total_amount", "NUMERIC(12,2)"),
        ("order_status", "TEXT"),
        ("payment_method", "TEXT"),
        ("created_at", "TIMESTAMP"),
    ],
    "order_items": [
        ("item_id", "TEXT PRIMARY KEY"),
        ("order_id", "TEXT"),
        ("product_id", "TEXT"),
        ("quantity", "INTEGER"),
        ("unit_price", "NUMERIC(12,2)"),
    ],
}

# Column names per table (for INSERT statements)
_TABLE_FIELDS = {
    "users": ["user_id", "full_name", "email", "phone_number",
              "customer_city", "customer_state", "loyalty_tier", "created_at"],
    "products": ["product_id", "product_name", "category", "cost_price"],
    "orders": ["order_id", "user_id", "total_amount", "order_status",
               "payment_method", "created_at"],
    "order_items": ["item_id", "order_id", "product_id", "quantity", "unit_price"],
}


# =========================================================
# 1. USERS
# =========================================================
def generate_users(rng: random.Random, sample_frac: float = 1.0) -> list[dict]:
    """Tạo users từ olist_customers + payments (loyalty) + orders (created_at)."""
    logger.info("=== Generating users ===")
    raw = _read_csv("olist_customers_dataset.csv")

    # Deduplicate theo customer_unique_id
    seen: dict[str, dict] = {}
    cid_to_uid: dict[str, str] = {}
    for row in raw:
        uid = row["customer_unique_id"]
        cid_to_uid[row["customer_id"]] = uid
        if uid not in seen:
            seen[uid] = row

    customers = list(seen.values())
    logger.info("  Customers unique: %d", len(customers))

    # created_at = MIN(order_purchase_timestamp) per customer
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

    # loyalty_tier = percentile trên SUM(payment_value)
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

    # Tính ngưỡng percentile
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
        full_name = deterministic_full_name(uid)
        email = deterministic_email(uid, full_name)
        phone = deterministic_phone(uid)

        if uid in min_timestamp:
            created_at = shift_timestamp(min_timestamp[uid])
        else:
            h = int(hashlib.md5(uid.encode()).hexdigest()[:8], 16)
            days_offset = h % 540
            raw_ts = (datetime(2017, 1, 1) + timedelta(days=days_offset)).strftime("%Y-%m-%d %H:%M:%S")
            created_at = shift_timestamp(raw_ts)

        # Gán loyalty tier theo percentile tổng chi tiêu
        spend = total_spend.get(uid, 0.0)
        if spend <= 0:
            tier = ""  # NULL — chưa có đơn
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
            "email": email if rng.random() > 0.10 else "",      # ~10% NULL
            "phone_number": phone if rng.random() > 0.15 else "",  # ~15% NULL
            "customer_city": row.get("customer_city", ""),
            "customer_state": row.get("customer_state", ""),
            "loyalty_tier": tier,
            "created_at": created_at,
        })

    logger.info("  Generated %d users", len(users))
    return users


# =========================================================
# 2. PRODUCTS
# =========================================================
def generate_products(rng: random.Random, order_items: list[dict] | None = None,
                      sample_frac: float = 1.0) -> list[dict]:
    """Tạo products từ olist_products + bảng dịch category."""
    logger.info("=== Generating products ===")
    raw = _read_csv("olist_products_dataset.csv")
    translations = _load_category_translations()

    if sample_frac < 1.0:
        k = max(1, int(len(raw) * sample_frac))
        raw = rng.sample(raw, k)

    # Tính avg price từ order_items
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

        h = int(hashlib.md5(pid.encode()).hexdigest()[:6], 16)
        name = f"{cat_translated} Item #{h % 10000}" if cat_translated else f"Product #{h % 10000}"

        # cost_price = 70% avg selling price, fallback random
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

    logger.info("  Generated %d products", len(products))
    return products


def _load_category_translations() -> dict[str, str]:
    """Đọc bảng dịch category PT → EN."""
    try:
        rows = _read_csv("product_category_name_translation.csv")
    except FileNotFoundError:
        return {}
    return {
        row["product_category_name"]: row["product_category_name_english"]
        for row in rows
        if row.get("product_category_name") and row.get("product_category_name_english")
    }


# =========================================================
# 3. ORDERS
# =========================================================
def generate_orders(rng: random.Random, user_ids: set[str],
                    order_items_raw: list[dict],
                    sample_frac: float = 1.0) -> tuple[list[dict], dict[str, str]]:
    """Tạo orders từ olist_orders + payments. Trả về (orders, order_to_user_map)."""
    logger.info("=== Generating orders ===")
    raw_orders = _read_csv("olist_orders_dataset.csv")
    raw_payments = _read_csv("olist_order_payments_dataset.csv")

    # payment_method = primary payment (sequential=1)
    payment_by_order: dict[str, str] = {}
    for row in raw_payments:
        oid = row["order_id"]
        seq = row.get("payment_sequential", "")
        if seq == "1":
            raw_method = row.get("payment_type", "")
            payment_by_order[oid] = PAYMENT_MAP.get(raw_method, "Credit Card")

    # total_amount = SUM(payment_value) per order
    total_by_order: dict[str, float] = {}
    for row in raw_payments:
        oid = row.get("order_id", "")
        try:
            val = float(row.get("payment_value", "0"))
        except (ValueError, TypeError):
            continue
        total_by_order[oid] = total_by_order.get(oid, 0.0) + val

    # Map customer_id → customer_unique_id
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
        created_at = shift_timestamp(row.get("order_purchase_timestamp", ""))
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

    logger.info("  Generated %d orders", len(orders))
    return orders, order_to_user


# =========================================================
# 4. ORDER ITEMS
# =========================================================
def generate_order_items(rng: random.Random, order_items_raw: list[dict],
                         sample_frac: float = 1.0) -> list[dict]:
    """Tạo order_items từ olist_order_items."""
    logger.info("=== Generating order_items ===")

    if sample_frac < 1.0:
        k = max(1, int(len(order_items_raw) * sample_frac))
        order_items_raw = rng.sample(order_items_raw, k)

    items = []
    for row in order_items_raw:
        oid = row.get("order_id", "")
        seq = row.get("order_item_id", "1")
        pid = row.get("product_id", "")
        try:
            price = float(row.get("price", "0"))
        except (ValueError, TypeError):
            price = 0.0

        items.append({
            "item_id": f"{oid}_{seq}",
            "order_id": oid,
            "product_id": pid,
            "quantity": 1,
            "unit_price": round(price, 2),
        })

    logger.info("  Generated %d order_items", len(items))
    return items


# =========================================================
# INIT.SQL WRITER
# =========================================================

def _sql_escape(value: str) -> str:
    """Escape a value for PostgreSQL literal."""
    if not value:
        return "NULL"
    return "'" + value.replace("'", "''") + "'"


def _write_inserts(out, table_name: str, rows: list[dict]) -> None:
    """Write batched INSERT statements for a table."""
    fields = _TABLE_FIELDS[table_name]
    col_names = ", ".join(fields)
    batch_size = 1000

    for batch_start in range(0, len(rows), batch_size):
        batch = rows[batch_start : batch_start + batch_size]
        out.write(f"INSERT INTO {table_name} ({col_names}) VALUES\n")
        value_lines = []
        for row in batch:
            vals = []
            for field in fields:
                v = str(row.get(field, ""))
                if v == "":
                    vals.append("NULL")
                else:
                    vals.append(_sql_escape(v))
            value_lines.append(f"  ({', '.join(vals)})")
        out.write(",\n".join(value_lines))
        out.write(";\n\n")


def write_init_sql(
    users: list[dict],
    products: list[dict],
    orders: list[dict],
    order_items: list[dict],
) -> Path:
    """Write init.sql: all CREATE TABLEs first, then all INSERTs."""
    logger.info("=== Tạo init.sql cho PostgreSQL ===")
    _INIT_SQL_PATH.parent.mkdir(parents=True, exist_ok=True)

    all_data = {
        "users": users,
        "products": products,
        "orders": orders,
        "order_items": order_items,
    }

    with open(_INIT_SQL_PATH, "w", encoding="utf-8") as out:
        out.write("-- Auto-generated by generate_sql_source.py\n")
        out.write("-- Do not edit manually — regenerate with: python -m generate_fake_data.run_all\n\n")

        # ── Phase 1: ALL CREATE TABLE statements ──
        out.write("-- " + "=" * 60 + "\n")
        out.write("-- DDL: CREATE TABLES\n")
        out.write("-- " + "=" * 60 + "\n\n")

        for table_name, columns in _TABLE_SCHEMAS.items():
            col_defs = ", ".join(f"{name} {dtype}" for name, dtype in columns)
            out.write(f"DROP TABLE IF EXISTS {table_name} CASCADE;\n")
            out.write(f"CREATE TABLE {table_name} ({col_defs});\n\n")

        # ── Phase 2: ALL INSERT statements ──
        out.write("-- " + "=" * 60 + "\n")
        out.write("-- DML: INSERT DATA\n")
        out.write("-- " + "=" * 60 + "\n\n")

        for table_name, rows in all_data.items():
            if not rows:
                continue
            out.write(f"-- {table_name}: {len(rows)} rows\n")
            _write_inserts(out, table_name, rows)
            logger.info("  %s: %d rows", table_name, len(rows))

    logger.info("  Đã ghi → %s", _INIT_SQL_PATH)
    return _INIT_SQL_PATH


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    parser = argparse.ArgumentParser(description="Tạo init.sql cho PostgreSQL từ Olist sample_data")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--sample-frac", type=float, default=1.0, help="Tỉ lệ lấy mẫu (0.0–1.0)")
    args = parser.parse_args()

    rng = random.Random(args.seed)
    logger.info("Bắt đầu tạo dữ liệu SQL (seed=%d, sample=%.0f%%)",
                args.seed, args.sample_frac * 100)

    order_items_raw = _read_csv("olist_order_items_dataset.csv")
    users = generate_users(rng, args.sample_frac)
    user_ids = {u["user_id"] for u in users}
    products = generate_products(rng, order_items_raw, args.sample_frac)
    orders, _ = generate_orders(rng, user_ids, order_items_raw, args.sample_frac)
    items = generate_order_items(rng, order_items_raw, args.sample_frac)

    write_init_sql(users, products, orders, items)

    logger.info("✅ Hoàn tất! init.sql tại: %s", _INIT_SQL_PATH)


if __name__ == "__main__":
    main()
