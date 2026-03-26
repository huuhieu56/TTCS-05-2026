"""FastAPI server — serves pre-generated clickstream events via REST API.

Supports:
- Seed data loading at startup
- Batch event ingestion (POST /events/batch) for daily data
- Filtering by timestamp (GET /events?since=...)
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("source-api")

app = FastAPI(title="Clickstream Event Source", version="2.0.0")

_SEED_FILE = Path(__file__).parent / "seed_events.json"
_DAILY_DIR = Path(__file__).parent / "daily_events"
_events: list[dict] = []


@app.on_event("startup")
async def _load_seed_data() -> None:
    global _events
    # Load seed file
    if _SEED_FILE.exists():
        with open(_SEED_FILE, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    _events.append(json.loads(line))
        logger.info("Loaded %d seed events from %s", len(_events), _SEED_FILE)

    # Load any daily event files
    if _DAILY_DIR.exists():
        for daily_file in sorted(_DAILY_DIR.glob("events_*.json")):
            count = 0
            with open(daily_file, encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        _events.append(json.loads(line))
                        count += 1
            logger.info("Loaded %d daily events from %s", count, daily_file.name)

    logger.info("Total events loaded: %d", len(_events))


@app.get("/health", tags=["Health"])
async def health():
    return {"status": "ok", "total_events": len(_events)}


@app.get("/events/count", tags=["Events"])
async def events_count():
    return {"total": len(_events)}


@app.get("/events", tags=["Events"])
async def get_events(
    limit: int = Query(default=1000, ge=1, le=10000),
    offset: int = Query(default=0, ge=0),
    since: str | None = Query(default=None, description="ISO timestamp to filter events after"),
):
    if since:
        # Filter events with timestamp >= since
        filtered = [
            e for e in _events
            if e.get("timestamp", "") >= since
        ]
        page = filtered[offset : offset + limit]
        return JSONResponse(content={
            "data": page,
            "total": len(filtered),
            "limit": limit,
            "offset": offset,
            "has_more": offset + limit < len(filtered),
            "filtered_by_since": since,
        })

    page = _events[offset : offset + limit]
    return JSONResponse(content={
        "data": page,
        "total": len(_events),
        "limit": limit,
        "offset": offset,
        "has_more": offset + limit < len(_events),
    })


class EventBatch(BaseModel):
    """Model for batch event ingestion — accepts a list of event dicts."""
    # Using __root__ pattern for backwards compat; we also accept raw list
    pass


@app.post("/events/batch", tags=["Events"])
async def ingest_batch(events: list[dict[str, Any]]):
    """Accept a batch of new events and append to the in-memory store.

    Also persists them to a daily file so they survive restarts.
    """
    if not events:
        return JSONResponse(content={"accepted": 0}, status_code=200)

    _events.extend(events)

    # Persist to daily file
    _DAILY_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now().strftime("%Y%m%d")
    daily_path = _DAILY_DIR / f"events_{today}.json"
    with open(daily_path, "a", encoding="utf-8") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False) + "\n")

    logger.info("Ingested %d events (total: %d), persisted to %s", len(events), len(_events), daily_path.name)
    return JSONResponse(content={"accepted": len(events), "total": len(_events)})
