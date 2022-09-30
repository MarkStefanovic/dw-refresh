import asyncio
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
    concurrent_procs: int,
    schema: str,
    connection_string: str,
    days_logs_to_keep: int,
    refresh_proc_name_patterns: list[str],
) -> None:
    start_time = datetime.datetime.now()

    pool: asyncpg.Pool = await asyncpg.create_pool(connection_string, min_size=1, max_size=concurrent_procs)

    try:
        initial_context = {
            "incremental": incremental,
            "concurrent_procs": concurrent_procs,
            "schema": schema,
            "refresh_proc_name_patterns": refresh_proc_name_patterns,
        }

        batch_id = await db.batch_started(pool=pool, context=initial_context)

        try:
            await db.cleanup(pool=pool, days_logs_to_keep=days_logs_to_keep)

            for pattern in refresh_proc_name_patterns:
                await run_procs_matching_pattern(
                    pool=pool,
                    batch_id=batch_id,
                    schema=schema,
                    concurrent_procs=concurrent_procs,
                    like=pattern,
                    proc_args={"p_incremental": incremental},
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
                    context={
                        "incremental": incremental,
                        "max_connections": concurrent_procs,
                        "schema": schema,
                    },
                )
            except Exception as e:
                logger.exception(e)
                raise
    except:  # noqa
        await asyncio.wait_for(pool.close(), 10)
        raise
