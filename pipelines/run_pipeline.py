"""Pipeline runner: Extract -> Transform -> Load."""

from extract import run_extract
from transform import run_transform
from load import run_load


def run_pipeline() -> None:
    run_extract()
    run_transform()
    run_load()


if __name__ == "__main__":
    run_pipeline()
