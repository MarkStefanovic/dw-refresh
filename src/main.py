import argparse
import asyncio
import datetime
import sys

from loguru import logger

from src import adapter, service


# noinspection PyShadowingNames
async def refresh(*, concurrent_procs: int, incremental: bool, days_logs_to_keep: int) -> None:
    config_path = adapter.fs.get_config_path()
    await service.refresh(
        incremental=incremental,
        concurrent_procs=concurrent_procs,
        connection_string=adapter.config.get_connection_string(config_path=config_path),
        schema=adapter.config.get_schema_name(config_path=config_path),
        days_logs_to_keep=days_logs_to_keep,
        refresh_proc_name_patterns=adapter.config.get_refresh_proc_name_patterns(config_path=config_path),
    )


async def check(*, concurrent_procs: int, days_logs_to_keep: int = 3) -> None:
    config_path = adapter.fs.get_config_path()
    await service.check(
        concurrent_procs=concurrent_procs,
        connection_string=adapter.config.get_connection_string(config_path=config_path),
        schema=adapter.config.get_schema_name(config_path=config_path),
        days_logs_to_keep=days_logs_to_keep,
        check_refresh_proc_name_patterns=adapter.config.get_check_refresh_proc_name_patterns(config_path=config_path),
    )

if __name__ == '__main__':
    adapter.fs.get_log_folder().mkdir(exist_ok=True)

    logger.add(sys.stdout, level="DEBUG")
    logger.add(adapter.fs.get_log_folder() / "info.log", rotation="5 MB", retention="7 days", level="INFO")
    logger.add(adapter.fs.get_log_folder() / "error.log", rotation="5 MB", retention="7 days", level="ERROR")

    try:
        logger.info("Starting dw-refresh...")

        parser = argparse.ArgumentParser(description="Utilities to manage refreshes of the data-warehouse.")
        subparser = parser.add_subparsers(dest="command", required=True)

        check_parser = subparser.add_parser("check", help="Run test procedures against the data-warehouse.")
        check_parser.add_argument("--days-logs-to-keep", type=int, default=3)
        check_parser.add_argument("--concurrent-procs", type=int, required=True)

        refresh_parser = subparser.add_parser("refresh", help="Refresh the data-warehouse.")
        refresh_parser.add_argument("--full", action="store_true", help="Run a full refresh.")
        refresh_parser.add_argument("--days-logs-to-keep", type=int, default=3)
        refresh_parser.add_argument("--concurrent-procs", type=int, required=True)

        args = parser.parse_args(sys.argv[1:])

        start = datetime.datetime.now()

        loop = asyncio.new_event_loop()
        try:
            if args.command == "check":
                logger.info("Running check...")
                loop.run_until_complete(
                    check(
                        days_logs_to_keep=args.days_logs_to_keep,
                        concurrent_procs=args.concurrent_procs,
                    )
                )
            else:
                incremental = not args.full
                logger.info(f"Running refresh({incremental})...")
                loop.run_until_complete(
                    refresh(
                        incremental=incremental,
                        days_logs_to_keep=args.days_logs_to_keep,
                        concurrent_procs=args.concurrent_procs,
                    )
                )
        except:  # noqa
            loop.close()
            raise
        seconds = (datetime.datetime.now() - start).total_seconds()
        logger.info(f"dw-refresh completed in {seconds} seconds.")
        sys.exit(0)
    except Exception as e:
        logger.exception(e)
        sys.exit(-1)
