"""Pipeline runner: Extract -> Transform -> Load with logging and CLI support."""

from __future__ import annotations

import argparse
import logging
import sys
import time

from pipelines.settings import load_config
from pipelines.extract import run_extract
from pipelines.transform import run_transform
from pipelines.load import run_load

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("pipeline")

STAGES = {
    "extract": run_extract,
    "transform": run_transform,
    "load": run_load,
}


def run_pipeline(stages: list[str] | None = None) -> None:
    config = load_config()
    target_stages = stages or list(STAGES.keys())

    logger.info("Pipeline starting — stages: %s", target_stages)
    start = time.time()

    for stage_name in target_stages:
        runner = STAGES.get(stage_name)
        if runner is None:
            logger.error("Unknown stage: %s. Valid: %s", stage_name, list(STAGES.keys()))
            sys.exit(1)

        logger.info(">>> Stage [%s] starting", stage_name)
        stage_start = time.time()
        runner(config)
        logger.info(">>> Stage [%s] completed in %.1fs", stage_name, time.time() - stage_start)

    logger.info("Pipeline finished in %.1fs", time.time() - start)


def main() -> None:
    parser = argparse.ArgumentParser(description="TTCS Customer 360 Data Pipeline")
    parser.add_argument(
        "--stage",
        choices=list(STAGES.keys()),
        nargs="+",
        help="Run specific stage(s). Default: all stages in order.",
    )
    args = parser.parse_args()
    run_pipeline(args.stage)


if __name__ == "__main__":
    main()
