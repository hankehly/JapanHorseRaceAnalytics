from pathlib import Path


def get_base_dir() -> Path:
    return Path(__file__).parent.parent.parent.resolve()


def get_data_dir() -> Path:
    return get_base_dir() / "data"
