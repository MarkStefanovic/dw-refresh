import functools
import os
import pathlib
import sys


__all__ = ("get_assets_folder", "get_config_path", "get_log_folder")


@functools.lru_cache(maxsize=1)
def _get_root_dir() -> pathlib.Path:
    if getattr(sys, "frozen", False):
        return pathlib.Path(os.path.dirname(sys.executable))

    return next(fp for fp in pathlib.Path(__file__).parents if fp.name == "dw-refresh")


@functools.lru_cache(maxsize=1)
def get_assets_folder() -> pathlib.Path:
    folder = _get_root_dir() / "assets"
    assert folder.exists(), f"An assets folder was not found at {_get_root_dir().resolve()}."
    return folder


@functools.lru_cache(maxsize=1)
def get_config_path() -> pathlib.Path:
    return _get_root_dir() / "assets" / "config.json"


@functools.lru_cache(maxsize=1)
def get_log_folder() -> pathlib.Path:
    folder = _get_root_dir() / "logs"
    folder.mkdir(exist_ok=True)
    return folder
