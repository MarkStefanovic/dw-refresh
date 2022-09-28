import argparse
import asyncio
import datetime
import logging
import sys

from loguru import logger

from src import adapter, service


# noinspection PyShadowingNames
async def refresh(*, incremental: bool = True, days_logs_to_keep: int = 3) -> None:
    config_path = adapter.fs.get_config_path()
    await service.refresh(
        incremental=incremental,
        max_connections=adapter.config.get_max_connections(config_path=config_path),
        connection_string=adapter.config.get_connection_string(config_path=config_path),
        schema=adapter.config.get_schema_name(config_path=config_path),
        days_logs_to_keep=days_logs_to_keep,
    )


async def check(*, days_logs_to_keep: int = 3) -> None:
    config_path = adapter.fs.get_config_path()
    await service.check(
        max_connections=adapter.config.get_max_connections(config_path=config_path),
        connection_string=adapter.config.get_connection_string(config_path=config_path),
        schema=adapter.config.get_schema_name(config_path=config_path),
        days_logs_to_keep=days_logs_to_keep,
    )

if __name__ == '__main__':
    adapter.fs.get_log_folder().mkdir(exist_ok=True)

    logger.add(adapter.fs.get_log_folder() / "info.log", rotation="5 MB", retention="7 days", level="INFO")
    logger.add(adapter.fs.get_log_folder() / "error.log", rotation="5 MB", retention="7 days", level="ERROR")

    if getattr(sys, "frozen", False):
        logger.add(sys.stderr, format="{time} {level} {message}", level=logging.DEBUG)

    try:
        logger.info("Starting dw-refresh...")

        parser = argparse.ArgumentParser(description="Utilities to manage refreshes of the data-warehouse.")
        subparser = parser.add_subparsers(dest="command", required=True)

        check_parser = subparser.add_parser("check", help="Run test procedures against the data-warehouse.")
        check_parser.add_argument("--days-logs-to-keep", type=int, required=True, default=3)

        refresh_parser = subparser.add_parser("refresh", help="Refresh the data-warehouse.")
        refresh_parser.add_argument("--full", action="store_true", help="Run a full refresh.")
        refresh_parser.add_argument("--days-logs-to-keep", type=int, required=True, default=3)

        args = parser.parse_args(sys.argv[1:])

        start = datetime.datetime.now()
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        if args.command == "check":
            logger.info("Running check...")
            asyncio.run(check())
        else:
            incremental = bool(args.incremental)
            logger.info(f"Running refresh({incremental})...")
            asyncio.run(refresh(incremental=incremental))
        seconds = (datetime.datetime.now() - start).total_seconds()
        logger.info(f"dw-refresh completed in {seconds} seconds.")
        sys.exit(0)
    except Exception as e:
        logger.exception(e)
        sys.exit(-1)

