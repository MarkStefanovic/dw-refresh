import datetime
import traceback

import asyncpg
from loguru import logger

from src.adapter import db
from src.service.run_procs import run_procs_matching_pattern

__all__ = ("check",)


async def check(
    *,
    max_connections: int,
    schema: str,
    connection_string: str,
    days_logs_to_keep: int,
) -> None:
    start_time = datetime.datetime.now()

    pool = await asyncpg.create_pool(connection_string)

    initial_context = {
        "max_connections": max_connections,
        "schema": schema,
    }

    batch_id = await db.batch_started(pool=pool, context=initial_context)

    try:
        await db.cleanup(pool=pool, days_logs_to_keep=days_logs_to_keep)

        await run_procs_matching_pattern(
            pool=pool,
            batch_id=batch_id,
            schema=schema,
            max_connections=max_connections,
            like=r"check_refresh\_%",
            proc_args={},
        )

        execution_millis = int((datetime.datetime.now() - start_time).total_seconds() * 1000)

        await db.batch_succeeded(
            pool=pool,
            batch_id=batch_id,
            execution_millis=execution_millis,
            context=initial_context,
        )
    except Exception as e:
        try:
            await db.batch_failed(
                pool=pool,
                batch_id=batch_id,
                error_message=f"{e!s}\n{traceback.format_exc()}",
                context=initial_context,
            )
        except Exception as e:
            logger.exception(e)
            raise

