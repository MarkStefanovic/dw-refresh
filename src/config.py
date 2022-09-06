import functools
import json
import pathlib
import typing

from src import fs

__all__ = ("get_connection_string", "get_max_connections", "get_schema_name")


@functools.lru_cache(maxsize=1)
def _get_config_file_contents(*, config_path: pathlib.Path = fs.get_config_path()) -> dict[str, typing.Hashable]:
    with config_path.open("r") as fh:
        return json.load(fh)


@functools.lru_cache(maxsize=1)
def get_connection_string(*, config_path: pathlib.Path = fs.get_config_path()) -> str:
    return typing.cast(str, _get_config_file_contents(config_path=config_path)["connection-string"])


@functools.lru_cache(maxsize=1)
def get_max_connections(*, config_path: pathlib.Path = fs.get_config_path()) -> int:
    return typing.cast(int, _get_config_file_contents(config_path=config_path)["max-connections"])


@functools.lru_cache(maxsize=1)
def get_schema_name(*, config_path: pathlib.Path = fs.get_config_path()) -> str:
    return typing.cast(str, _get_config_file_contents(config_path=config_path)["schema-name"])
