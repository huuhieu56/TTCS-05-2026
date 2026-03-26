"""Shared constants and helpers used across all fake-data generator modules."""

from __future__ import annotations

import csv
import hashlib
import logging
from datetime import datetime, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_SAMPLE_DIR = _PROJECT_ROOT / "sample_data"

# ---------------------------------------------------------------------------
# Date-shift helpers  (move Olist 2017-2018 dates to the present)
# ---------------------------------------------------------------------------

# The latest date in the Olist dataset (order_purchase_timestamp).
_OLIST_MAX_DATE = datetime(2018, 10, 17)

# Cache the delta so it is only computed once per process.
_DATE_SHIFT_DELTA: timedelta | None = None


def calculate_date_shift(*, anchor_now: datetime | None = None) -> timedelta:
    """Return the timedelta that shifts Olist dates to 'near today'.

    The shift is calculated so that ``_OLIST_MAX_DATE + delta == anchor_now``.
    ``anchor_now`` defaults to the current UTC time.
    """
    global _DATE_SHIFT_DELTA  # noqa: PLW0603
    if _DATE_SHIFT_DELTA is None:
        now = anchor_now or datetime.now()
        _DATE_SHIFT_DELTA = now - _OLIST_MAX_DATE
        logger.info(
            "Date shift: Olist max %s → now %s  (delta=%d days)",
            _OLIST_MAX_DATE.date(),
            now.date(),
            _DATE_SHIFT_DELTA.days,
        )
    return _DATE_SHIFT_DELTA


def reset_date_shift() -> None:
    """Reset cached delta (useful for tests)."""
    global _DATE_SHIFT_DELTA  # noqa: PLW0603
    _DATE_SHIFT_DELTA = None


def shift_timestamp(ts_str: str, fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    """Parse *ts_str*, add the date-shift delta, and return the shifted string."""
    if not ts_str or not ts_str.strip():
        return ts_str
    delta = calculate_date_shift()
    try:
        dt = datetime.strptime(ts_str.strip(), fmt)
        return (dt + delta).strftime(fmt)
    except ValueError:
        # Try ISO 8601 variant
        for alt_fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
            try:
                dt = datetime.strptime(ts_str.strip(), alt_fmt)
                return (dt + delta).strftime(alt_fmt)
            except ValueError:
                continue
        logger.warning("Could not parse timestamp for shifting: %r", ts_str)
        return ts_str

# ---------------------------------------------------------------------------
# Name / contact helpers  (shared by sql_source and excel_source generators)
# ---------------------------------------------------------------------------

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

_ACCENT_MAP = [
    ("ã", "a"), ("í", "i"), ("á", "a"), ("ú", "u"), ("ó", "o"),
    ("ê", "e"), ("é", "e"), ("â", "a"), ("ô", "o"), ("õ", "o"),
    ("à", "a"), ("ç", "c"), ("ü", "u"),
]


def _remove_accents(text: str) -> str:
    for accented, plain in _ACCENT_MAP:
        text = text.replace(accented, plain)
    return text


def deterministic_full_name(entity_id: str) -> str:
    """Return a reproducible fake full name derived from *entity_id*."""
    h = int(hashlib.md5(entity_id.encode()).hexdigest(), 16)
    first = FIRST_NAMES[h % len(FIRST_NAMES)]
    last = LAST_NAMES[(h >> 8) % len(LAST_NAMES)]
    return f"{first} {last}"


def deterministic_email(entity_id: str, full_name: str | None = None) -> str:
    """Return a reproducible fake email derived from *entity_id*.

    If *full_name* is provided, the local part is derived from the name;
    otherwise a name is generated deterministically from *entity_id*.
    """
    if full_name is None:
        full_name = deterministic_full_name(entity_id)
    name_part = _remove_accents(full_name.lower().replace(" ", "."))
    h = int(hashlib.md5(entity_id.encode()).hexdigest()[:8], 16)
    domain = EMAIL_DOMAINS[h % len(EMAIL_DOMAINS)]
    suffix = entity_id[:4]
    return f"{name_part}.{suffix}@{domain}"


def deterministic_phone(entity_id: str) -> str:
    """Return a reproducible fake Brazilian phone number derived from *entity_id*."""
    h = int(hashlib.md5(entity_id.encode()).hexdigest()[:12], 16)
    ddd = 11 + (h % 79)
    number = 900_000_000 + (h % 100_000_000)
    return f"+55 {ddd} {number}"


# ---------------------------------------------------------------------------
# CSV I/O helpers
# ---------------------------------------------------------------------------

def read_csv(filename: str) -> list[dict]:
    """Read a CSV from sample_data/ and return a list of row dicts."""
    path = _SAMPLE_DIR / filename
    if not path.exists():
        raise FileNotFoundError(f"Sample data file not found: {path}")
    with open(path, encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(output_dir: Path, filename: str, rows: list[dict], fieldnames: list[str]) -> Path:
    """Write *rows* to *output_dir/filename* as a CSV with a header."""
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / filename
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    logger.info("Wrote %d rows → %s", len(rows), path)
    return path
