import datetime
import traceback

import asyncpg
from loguru import logger

from src.adapter import db
from src.service.run_procs import run_procs_matching_pattern

__all__ = ("refresh",)


async def refresh(
    *,
    incremental: bool,
    max_connections: int,
    schema: str,
    connection_string: str,
    days_logs_to_keep: int,
) -> None:
    start_time = datetime.datetime.now()

    pool = await asyncpg.create_pool(connection_string)

    initial_context = {
        "incremental": incremental,
        "max_connections": max_connections,
        "schema": schema,
    }

    batch_id = await db.batch_started(pool=pool, context=initial_context)

    try:
        await db.cleanup(pool=pool, days_logs_to_keep=days_logs_to_keep)

        for pattern in (
            r"refresh\_p\_%",
            r"refresh\_h\_%",
            r"refresh\_l\_%",
            r"refresh\_sal\_%",
            r"refresh\_s\_%",
            r"refresh\_%\_dim",
            r"refresh\_%\_fact",
        ):
            await run_procs_matching_pattern(
                pool=pool,
                batch_id=batch_id,
                schema=schema,
                max_connections=max_connections,
                like=pattern,
                proc_args={"p_incremental": incremental},
            )

            execution_millis = int((datetime.datetime.now() - start_time).total_seconds() * 1000)

            await db.batch_succeeded(
                pool=pool,
                batch_id=batch_id,
                execution_millis=execution_millis,
                context=initial_context | {"pattern": pattern}
            )
    except Exception as e:
        try:
            await db.batch_failed(
                pool=pool,
                batch_id=batch_id,
                error_message=f"{e!s}\n{traceback.format_exc()}",
                context={
                    "incremental": incremental,
                    "max_connections": max_connections,
                    "schema": schema,
                },
            )
        except Exception as e:
            logger.exception(e)
            raise
