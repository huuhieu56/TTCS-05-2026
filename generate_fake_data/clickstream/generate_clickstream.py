"""Wrapper — chạy generate_api_source để tạo clickstream data.

Xem generate_fake_data/generate_api_source.py để biết chi tiết.

Usage:
    python -m generate_fake_data.clickstream.generate_clickstream [--num-sessions 5000]
"""

from __future__ import annotations

import sys
from pathlib import Path

# Thêm project root vào path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from generate_fake_data.generate_api_source import main

if __name__ == "__main__":
    # Mặc định chạy batch mode nếu không có argument
    if len(sys.argv) == 1:
        sys.argv.extend(["batch", "--num-sessions", "5000"])
    main()
