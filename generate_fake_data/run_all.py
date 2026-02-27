"""Chạy tất cả 3 generator cùng lúc — tạo data_source/ đầy đủ cho pipeline.

Tạo 3 loại nguồn dữ liệu:
  1. SQL  → data_source/sql/     (users.csv, products.csv, orders.csv, order_items.csv)
  2. Excel → data_source/excel/  (CS_Tickets.xlsx)
  3. API  → data_source/api/     (clickstream.json)

Usage:
    python -m generate_fake_data.run_all [--seed 42] [--sample-frac 1.0] [--num-sessions 5000]
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

# Đảm bảo project root nằm trong sys.path để import hoạt động
# khi chạy trực tiếp (python run_all.py) thay vì python -m
_PROJECT_ROOT = str(Path(__file__).resolve().parents[1])
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("run_all")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tạo toàn bộ dữ liệu giả (SQL + Excel + API) từ Olist sample_data"
    )
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--sample-frac", type=float, default=1.0,
                        help="Tỉ lệ lấy mẫu (0.0–1.0)")
    parser.add_argument("--num-sessions", type=int, default=5000,
                        help="Số sessions clickstream (mặc định: 5000)")
    args = parser.parse_args()

    start = time.time()
    logger.info("=" * 60)
    logger.info("BẮT ĐẦU TẠO DỮ LIỆU GIẢ")
    logger.info("  Seed: %d | Sample: %.0f%% | Sessions: %d",
                args.seed, args.sample_frac * 100, args.num_sessions)
    logger.info("=" * 60)

    # 1. SQL Source
    logger.info("")
    logger.info("━━━ [1/3] NGUỒN SQL ━━━")
    import random

    from generate_fake_data.generate_sql_source import (
        generate_users, generate_products, generate_orders,
        generate_order_items, _read_csv,
    )
    rng_sql = random.Random(args.seed)
    order_items_raw = _read_csv("olist_order_items_dataset.csv")
    users = generate_users(rng_sql, args.sample_frac)
    user_ids = {u["user_id"] for u in users}
    generate_products(rng_sql, order_items_raw, args.sample_frac)
    generate_orders(rng_sql, user_ids, order_items_raw, args.sample_frac)
    generate_order_items(rng_sql, order_items_raw, args.sample_frac)

    # 2. Excel Source
    logger.info("")
    logger.info("━━━ [2/3] NGUỒN EXCEL ━━━")
    from generate_fake_data.generate_excel_source import generate_cs_tickets
    rng_excel = random.Random(args.seed)
    generate_cs_tickets(rng_excel, args.sample_frac)

    # 3. API Source (batch)
    logger.info("")
    logger.info("━━━ [3/3] NGUỒN API (Clickstream) ━━━")
    from generate_fake_data.generate_api_source import run_batch
    batch_args = argparse.Namespace(seed=args.seed, num_sessions=args.num_sessions)
    run_batch(batch_args)

    elapsed = time.time() - start
    logger.info("")
    logger.info("=" * 60)
    logger.info("✅ HOÀN TẤT TẤT CẢ trong %.1f giây!", elapsed)
    logger.info("   data_source/sql/    → users, products, orders, order_items")
    logger.info("   data_source/excel/  → CS_Tickets.xlsx")
    logger.info("   data_source/api/    → clickstream.json")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
