"""Wrapper — chạy generate_api_source batch mode."""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from generate_fake_data.generate_api_source import main

if __name__ == "__main__":
    if len(sys.argv) == 1:
        sys.argv.extend(["batch", "--num-sessions", "5000"])
    main()
