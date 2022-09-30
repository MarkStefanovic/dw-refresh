import functools
import json
import pathlib
import typing

from src.adapter import fs

__all__ = (
    "get_check_refresh_proc_name_patterns",
    "get_connection_string",
    "get_schema_name",
    "get_refresh_proc_name_patterns",
)


@functools.lru_cache(maxsize=1)
def _get_config_file_contents(*, config_path: pathlib.Path = fs.get_config_path()) -> dict[str, typing.Hashable]:
    with config_path.open("r") as fh:
        return json.load(fh)


def get_connection_string(*, config_path: pathlib.Path = fs.get_config_path()) -> str:
    return typing.cast(str, _get_config_file_contents(config_path=config_path)["connection-string"])


def get_schema_name(*, config_path: pathlib.Path = fs.get_config_path()) -> str:
    return typing.cast(str, _get_config_file_contents(config_path=config_path)["schema-name"])


def get_check_refresh_proc_name_patterns(*, config_path: pathlib.Path = fs.get_config_path()) -> list[str]:
    patterns = typing.cast(list[str], _get_config_file_contents(config_path=config_path)["check-refresh-proc-name-patterns"])
    return [pattern.replace(r"\\", "\\") for pattern in patterns]


def get_refresh_proc_name_patterns(*, config_path: pathlib.Path = fs.get_config_path()) -> list[str]:
    return typing.cast(list[str], _get_config_file_contents(config_path=config_path)["refresh-proc-name-patterns"])
