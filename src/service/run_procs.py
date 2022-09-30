import datetime
import traceback
import typing

import asyncpg
from loguru import logger

from src.adapter import db

__all__ = ("run_procs_matching_pattern",)


async def run_procs_matching_pattern(
    *,
    pool: asyncpg.Pool,
    batch_id: int,
    schema: str,
    like: str,
    concurrent_procs: int,
    proc_args: dict[str, typing.Hashable],
) -> None:
    procs = await db.get_proc_names_by_pattern(pool=pool, schema=schema, pattern=like)
    await _run_procs(
        pool=pool,
        batch_id=batch_id,
        schema=schema,
        stored_procs=procs,
        concurrent_procs=concurrent_procs,
        proc_args=proc_args,
    )


async def _run_proc(
    *,
    pool: asyncpg.Pool,
    batch_id: int,
    schema: str,
    stored_proc: str,
    proc_args: dict[str, typing.Hashable],
) -> None:
    initial_context = {
        "batch_id": batch_id,
        "schema": schema,
        "stored_proc": stored_proc,
    } | proc_args

    proc_id = await db.proc_started(
        pool=pool,
        batch_id=batch_id,
        proc_name=stored_proc,
        context=initial_context,
    )

    try:
        start_time = datetime.datetime.now()

        await db.run_proc(pool=pool, schema=schema, proc_name=stored_proc, proc_args=proc_args)

        execution_millis = int((datetime.datetime.now() - start_time).total_seconds() * 1000)

        await db.proc_succeeded(
            pool=pool,
            proc_id=proc_id,
            execution_millis=execution_millis,
            context=initial_context,
        )
    except Exception as e:
        try:
            await db.proc_failed(
                pool=pool,
                proc_id=proc_id,
                error_message=(
                    f"An error occurred while running {schema}.{stored_proc}: {e!s}\n{traceback.format_exc()}"
                ),
                context=initial_context,
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
    concurrent_procs: int,
    proc_args: dict[str, typing.Hashable],
) -> None:
    tasks = [
        _run_proc(
            pool=pool,
            batch_id=batch_id,
            schema=schema,
            stored_proc=stored_proc,
            proc_args=proc_args,
        )
        for stored_proc in stored_procs
    ]

    await db.gather_with_limited_concurrency(concurrent_procs, *tasks)  # type: ignore
