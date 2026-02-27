"""Tạo dữ liệu nguồn API — clickstream events qua FastAPI + batch JSON file.

Có 2 chế độ:
  1. `batch`  : Tạo file JSON-lines trực tiếp vào data_source/api/clickstream.json
  2. `server` : Chạy FastAPI server, bắn 1 event mỗi phút qua endpoint

Đầu vào : sample_data/olist_customers_dataset.csv  (lấy customer_id)
           sample_data/olist_products_dataset.csv   (lấy product_id)
Đầu ra  : data_source/api/clickstream.json  (JSON-lines)

Usage:
    # Chế độ batch — tạo file 1 lần
    python -m generate_fake_data.generate_api_source batch --num-sessions 5000

    # Chế độ server — FastAPI bắn event tự động mỗi phút
    python -m generate_fake_data.generate_api_source server --port 8000

Yêu cầu server: pip install fastapi uvicorn
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("generate_api")

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SAMPLE_DIR = _PROJECT_ROOT / "sample_data"
_OUTPUT_DIR = _PROJECT_ROOT / "data_source" / "api"
_OUTPUT_FILE = _OUTPUT_DIR / "clickstream.json"

# --- Cấu hình ---
EVENT_TYPES = ["view_item", "add_to_cart", "cart_abandonment"]
DEVICE_OS_LIST = ["Android", "iOS", "Windows", "macOS", "Linux"]
# Xác suất chuyển đổi trong funnel
FUNNEL_PROBS = {
    "view_item": 0.40,       # 40% view → add_to_cart
    "add_to_cart": 0.35,     # 35% add → cart_abandonment
}

# Khoảng thời gian dữ liệu Olist
DATE_START = datetime(2017, 10, 1)
DATE_END = datetime(2018, 8, 31)


def _read_ids(filename: str, id_column: str) -> list[str]:
    """Đọc danh sách ID unique từ file CSV."""
    path = _SAMPLE_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Không tìm thấy file: {path}")
    ids: set[str] = set()
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            val = row.get(id_column, "").strip()
            if val:
                ids.add(val)
    return list(ids)


def _load_real_ids() -> tuple[list[str], list[str]]:
    """Load customer_unique_id và product_id thực từ Olist."""
    logger.info("Đang đọc IDs từ sample_data...")
    customer_ids = _read_ids("olist_customers_dataset.csv", "customer_unique_id")
    product_ids = _read_ids("olist_products_dataset.csv", "product_id")
    logger.info("  Customers: %d unique IDs", len(customer_ids))
    logger.info("  Products : %d unique IDs", len(product_ids))
    return customer_ids, product_ids


def generate_single_event(
    rng: random.Random,
    customer_ids: list[str],
    product_ids: list[str],
    session_id: str,
    event_type: str,
    base_time: datetime,
) -> dict[str, Any]:
    """Tạo 1 event clickstream theo đúng events.schema.json."""
    # user_id: 95% có, 5% anonymous (null)
    user_id = rng.choice(customer_ids) if rng.random() > 0.05 else None

    # product_id: null cho cart_abandonment ~30%, luôn có cho view/add
    if event_type == "cart_abandonment" and rng.random() < 0.30:
        product_id = None
    else:
        product_id = rng.choice(product_ids)

    # device_os: 97% có, 3% null
    device_os = rng.choice(DEVICE_OS_LIST) if rng.random() > 0.03 else None

    # time_spent_seconds: tùy loại event
    if rng.random() < 0.02:
        time_spent = None  # 2% null
    elif event_type == "view_item":
        time_spent = rng.randint(5, 300)
    elif event_type == "add_to_cart":
        time_spent = rng.randint(3, 60)
    else:  # cart_abandonment
        time_spent = rng.randint(1, 30)

    return {
        "event_id": str(uuid.uuid4()),
        "timestamp": base_time.strftime("%Y-%m-%dT%H:%M:%S.") + f"{rng.randint(0,999):03d}Z",
        "user_id": user_id,
        "session_id": session_id,
        "event_type": event_type,
        "product_id": product_id,
        "device_os": device_os,
        "time_spent_seconds": time_spent,
    }


def generate_session_events(
    rng: random.Random,
    customer_ids: list[str],
    product_ids: list[str],
    session_start: datetime,
) -> list[dict[str, Any]]:
    """Tạo 1 session gồm nhiều events theo funnel logic.

    Funnel: view_item → (40%) add_to_cart → (35%) cart_abandonment
    Mỗi session có 1–8 events.
    """
    session_id = str(uuid.uuid4())
    events: list[dict] = []
    current_time = session_start

    # Chọn 1 user_id cố định cho session (hoặc anonymous)
    if rng.random() > 0.05:
        session_user = rng.choice(customer_ids)
    else:
        session_user = None

    # Chọn device cố định cho session
    session_device = rng.choice(DEVICE_OS_LIST) if rng.random() > 0.03 else None

    # Số lượng view_item ban đầu: 1–6
    num_views = rng.randint(1, 6)

    for _ in range(num_views):
        event = generate_single_event(
            rng, customer_ids, product_ids, session_id, "view_item", current_time
        )
        # Override user/device cho consistency trong session
        event["user_id"] = session_user
        event["device_os"] = session_device
        events.append(event)
        current_time += timedelta(seconds=rng.randint(10, 120))

    # Có thêm add_to_cart không?
    if rng.random() < FUNNEL_PROBS["view_item"]:
        num_adds = rng.randint(1, 3)
        for _ in range(num_adds):
            event = generate_single_event(
                rng, customer_ids, product_ids, session_id, "add_to_cart", current_time
            )
            event["user_id"] = session_user
            event["device_os"] = session_device
            events.append(event)
            current_time += timedelta(seconds=rng.randint(5, 60))

        # Có cart_abandonment không?
        if rng.random() < FUNNEL_PROBS["add_to_cart"]:
            event = generate_single_event(
                rng, customer_ids, product_ids, session_id, "cart_abandonment", current_time
            )
            event["user_id"] = session_user
            event["device_os"] = session_device
            events.append(event)

    return events


# =========================================================
# CHẾ ĐỘ 1: BATCH — Tạo file JSON-lines 1 lần
# =========================================================
def run_batch(args: argparse.Namespace) -> None:
    """Tạo file clickstream.json chứa tất cả events."""
    rng = random.Random(args.seed)
    customer_ids, product_ids = _load_real_ids()

    logger.info("Bắt đầu tạo %d sessions (batch mode)...", args.num_sessions)
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    total_events = 0
    total_range = (DATE_END - DATE_START).total_seconds()

    with open(_OUTPUT_FILE, "w", encoding="utf-8") as f:
        for i in range(args.num_sessions):
            # Random thời điểm bắt đầu session
            offset = rng.random() * total_range
            session_start = DATE_START + timedelta(seconds=offset)

            events = generate_session_events(rng, customer_ids, product_ids, session_start)
            for event in events:
                f.write(json.dumps(event, ensure_ascii=False) + "\n")
                total_events += 1

            if (i + 1) % 1000 == 0:
                logger.info("  ... %d/%d sessions (%d events)",
                            i + 1, args.num_sessions, total_events)

    logger.info("✅ Hoàn tất batch! %d sessions → %d events", args.num_sessions, total_events)
    logger.info("   File: %s (%.1f MB)",
                _OUTPUT_FILE, _OUTPUT_FILE.stat().st_size / 1024 / 1024)


# =========================================================
# CHẾ ĐỘ 2: SERVER — FastAPI bắn event mỗi phút
# =========================================================
def run_server(args: argparse.Namespace) -> None:
    """Chạy FastAPI server, tự động tạo 1 event mỗi phút và lưu vào file."""
    try:
        import uvicorn
        from fastapi import FastAPI
        from fastapi.responses import JSONResponse
    except ImportError:
        logger.error("Cần cài: pip install fastapi uvicorn")
        return

    import asyncio
    import threading

    rng = random.Random(args.seed)
    customer_ids, product_ids = _load_real_ids()
    _OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    app = FastAPI(
        title="Clickstream Event Generator",
        description="API tạo clickstream events giả lập — bắn 1 event mỗi phút",
        version="1.0.0",
    )

    # Lưu trữ events gần đây (in-memory ring buffer)
    recent_events: list[dict] = []
    event_count = {"total": 0}

    def _emit_event() -> dict:
        """Tạo và lưu 1 event mới."""
        session_id = str(uuid.uuid4())
        event_type = rng.choices(
            EVENT_TYPES,
            weights=[0.60, 0.25, 0.15],  # 60% view, 25% add, 15% abandon
            k=1,
        )[0]
        event = generate_single_event(
            rng, customer_ids, product_ids, session_id, event_type, datetime.utcnow()
        )

        # Ghi vào file (append)
        with open(_OUTPUT_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

        # Lưu vào ring buffer
        recent_events.append(event)
        if len(recent_events) > 100:
            recent_events.pop(0)
        event_count["total"] += 1

        return event

    # Background task: bắn event mỗi phút
    def _background_emitter():
        """Thread chạy nền, tạo event mỗi 60 giây."""
        import time
        logger.info("🔄 Background emitter bắt đầu — 1 event / phút")
        while True:
            event = _emit_event()
            logger.info("📤 Event #%d: %s (type=%s, user=%s)",
                        event_count["total"],
                        event["event_id"][:8],
                        event["event_type"],
                        (event["user_id"] or "anonymous")[:8])
            time.sleep(60)

    @app.on_event("startup")
    async def startup():
        thread = threading.Thread(target=_background_emitter, daemon=True)
        thread.start()
        logger.info("🚀 FastAPI server đã khởi động, background emitter đang chạy")

    @app.get("/", tags=["Health"])
    async def root():
        return {
            "service": "Clickstream Event Generator",
            "total_events": event_count["total"],
            "output_file": str(_OUTPUT_FILE),
        }

    @app.get("/events/latest", tags=["Events"])
    async def get_latest_events(limit: int = 10):
        """Lấy N events gần nhất."""
        return JSONResponse(content=recent_events[-limit:])

    @app.post("/events/emit", tags=["Events"])
    async def emit_event_now():
        """Bắn 1 event ngay lập tức (ngoài lịch tự động)."""
        event = _emit_event()
        return JSONResponse(content=event)

    @app.get("/events/stats", tags=["Stats"])
    async def get_stats():
        """Thống kê events."""
        type_counts: dict[str, int] = {}
        for e in recent_events:
            t = e.get("event_type", "unknown")
            type_counts[t] = type_counts.get(t, 0) + 1
        return {
            "total_events": event_count["total"],
            "recent_count": len(recent_events),
            "type_distribution": type_counts,
        }

    logger.info("Khởi động FastAPI server tại http://0.0.0.0:%d", args.port)
    logger.info("  Docs: http://localhost:%d/docs", args.port)
    uvicorn.run(app, host="0.0.0.0", port=args.port, log_level="info")


# =========================================================
# MAIN
# =========================================================
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Tạo dữ liệu nguồn API (clickstream) từ Olist IDs"
    )
    subparsers = parser.add_subparsers(dest="mode", required=True)

    # Batch mode
    batch_parser = subparsers.add_parser("batch", help="Tạo file JSON-lines 1 lần")
    batch_parser.add_argument("--num-sessions", type=int, default=5000,
                              help="Số lượng sessions (mặc định: 5000)")
    batch_parser.add_argument("--seed", type=int, default=42, help="Random seed")

    # Server mode
    server_parser = subparsers.add_parser("server", help="Chạy FastAPI, bắn 1 event/phút")
    server_parser.add_argument("--port", type=int, default=8000, help="Port (mặc định: 8000)")
    server_parser.add_argument("--seed", type=int, default=42, help="Random seed")

    args = parser.parse_args()

    if args.mode == "batch":
        run_batch(args)
    elif args.mode == "server":
        run_server(args)


if __name__ == "__main__":
    main()
