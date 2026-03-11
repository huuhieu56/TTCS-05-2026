"""FastAPI server — serves pre-generated clickstream events via REST API."""

from __future__ import annotations

import json
import logging
from pathlib import Path

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("source-api")

app = FastAPI(title="Clickstream Event Source", version="1.0.0")

_SEED_FILE = Path(__file__).parent / "seed_events.json"
_events: list[dict] = []


@app.on_event("startup")
async def _load_seed_data() -> None:
    global _events
    if not _SEED_FILE.exists():
        logger.warning("Seed file not found: %s — server starts with empty data", _SEED_FILE)
        return
    with open(_SEED_FILE, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                _events.append(json.loads(line))
    logger.info("Loaded %d events from %s", len(_events), _SEED_FILE)


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
):
    page = _events[offset : offset + limit]
    return JSONResponse(content={
        "data": page,
        "total": len(_events),
        "limit": limit,
        "offset": offset,
        "has_more": offset + limit < len(_events),
    })
