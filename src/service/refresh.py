import datetime
import traceback

import asyncpg
from loguru import logger

from src.adapter import db

__all__ = ("refresh",)


async def refresh(
    *,
    incremental: bool,
    max_connections: int,
    schema: str,
    connection_string: str,
) -> None:
    start_time = datetime.datetime.now()

    pool = await asyncpg.create_pool(connection_string)
    batch_id = await db.batch_started(pool=pool)
    try:
        for pattern in (
            r"refresh\_p\_%",
            r"refresh\_h\_%",
            r"refresh\_l\_%",
            r"refresh\_sal\_%",
            r"refresh\_s\_%",
            r"refresh\_%\_dim",
            r"refresh\_%\_fact",
        ):
            await _run_procs_matching_pattern(
                pool=pool,
                batch_id=batch_id,
                schema=schema,
                incremental=incremental,
                max_connections=max_connections,
                like=pattern,
            )

            execution_millis = int((datetime.datetime.now() - start_time).total_seconds() * 1000)

            await db.batch_succeeded(
                pool=pool,
                batch_id=batch_id,
                execution_millis=execution_millis,
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


async def _run_proc(
    *,
    pool: asyncpg.Pool,
    batch_id: int,
    schema: str,
    stored_proc: str,
    incremental: bool,
) -> None:
    proc_id = await db.proc_started(pool=pool, batch_id=batch_id, proc_name=stored_proc)
    try:
        start_time = datetime.datetime.now()
        await db.run_proc(pool=pool, schema=schema, proc_name=stored_proc, incremental=incremental)
        execution_millis = int((datetime.datetime.now() - start_time).total_seconds() * 1000)
        await db.proc_succeeded(pool=pool, proc_id=proc_id, execution_millis=execution_millis)
    except Exception as e:
        try:
            await db.proc_failed(
                pool=pool,
                proc_id=proc_id,
                error_message=f"An error occurred while running {schema}.{stored_proc}: {e!s}\n{traceback.format_exc()}",
                context={
                    "schema": schema,
                    "stored_proc": stored_proc,
                    "incremental": incremental,
                },
            )
        except Exception as e:
            logger.exception(e)
            raise


async def _run_procs(
    *,
    pool: asyncpg.Pool,
    batch_id: int,
    schema: str,
    stored_procs: set[str],
    incremental: bool,
    max_connections: int,
) -> None:
    tasks = [
        _run_proc(
            pool=pool,
            batch_id=batch_id,
            schema=schema,
            stored_proc=stored_proc,
            incremental=incremental,
        )
        for stored_proc in stored_procs
    ]
    await db.gather_with_limited_concurrency(max_connections, *tasks)


async def _run_procs_matching_pattern(
    *,
    pool: asyncpg.Pool,
    batch_id: int,
    schema: str,
    like: str,
    incremental: bool,
    max_connections: int,
) -> None:
    procs = await db.get_proc_names_by_pattern(pool=pool, schema=schema, like=like)
    await _run_procs(
        pool=pool,
        batch_id=batch_id,
        schema=schema,
        stored_procs=procs,
        incremental=incremental,
        max_connections=max_connections,
    )
