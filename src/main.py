import asyncio
import datetime
import logging
import pathlib
import sys

from loguru import logger

from src import config, db, fs


async def main(*, incremental: bool = True, config_path: pathlib.Path = fs.get_config_path()) -> None:
    await db.refresh_dw(
        incremental=incremental,
        max_connections=config.get_max_connections(config_path=config_path),
        connection_string=config.get_connection_string(config_path=config_path),
        schema=config.get_schema_name(config_path=config_path),
    )


if __name__ == '__main__':
    fs.get_log_folder().mkdir(exist_ok=True)

    logger.add(fs.get_log_folder() / "info.log", rotation="5 MB", retention="7 days", level=logging.INFO)
    logger.add(fs.get_log_folder() / "error.log", rotation="5 MB", retention="7 days", level=logging.ERROR)

    if getattr(sys, "frozen", False):
        logger.add(sys.stderr, format="{time} {level} {message}", level=logging.DEBUG)

    try:
        logger.info("Starting dw-refresh...")

        if len(sys.argv) == 1:
            incr = True
        elif len(sys.argv) == 2:
            incr = bool(int(sys.argv[1]))
        else:
            raise Exception(f"dw-refresh accepts 1 argument (incremental = 0 or 1), but {len(sys.argv)} arguments were provided.")

        start = datetime.datetime.now()
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        asyncio.run(main(incremental=incr))
        seconds = (datetime.datetime.now() - start).total_seconds()
        logger.info(f"dw-refresh completed in {seconds} seconds.")
        sys.exit(0)
    except Exception as e:
        logger.exception(e)
        sys.exit(-1)

