import asyncio
import typing

import asyncpg

__all__ = (
    "batch_failed",
    "batch_started",
    "batch_succeeded",
    "gather_with_limited_concurrency",
    "get_proc_names_by_pattern",
    "proc_failed",
    "proc_started",
    "proc_succeeded",
    "run_proc",
)

_DEFAULT_TIMEOUT = 10


async def batch_failed(
    *,
    pool: asyncpg.Pool,
    batch_id: int,
    error_message: str,
    context: dict[str, typing.Hashable],
) -> int:
    async with pool.acquire(timeout=_DEFAULT_TIMEOUT) as con:
        return await con.execute(
            "CALL dwr.batch_failed(p_batch_id := $1, p_error_message := $2, p_context := $3);",
            batch_id, error_message, context,
        )


async def batch_started(*, pool: asyncpg.Pool) -> int:
    async with pool.acquire(timeout=_DEFAULT_TIMEOUT) as con:
        return await con.fetchval("SELECT * FROM dwr.batch_started() AS bid;")


async def batch_succeeded(*, pool: asyncpg.Pool, batch_id: int, execution_millis: int) -> int:
    async with pool.acquire(timeout=_DEFAULT_TIMEOUT) as con:
        return await con.execute(
            "CALL dwr.batch_succeeded (p_batch_id := $1, p_execution_millis := $2);",
            batch_id, execution_millis,
        )


async def gather_with_limited_concurrency(n: int, *tasks: asyncio.Task) -> tuple[asyncio.Future, ...]:
    semaphore = asyncio.Semaphore(n)

    async def wrap_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(wrap_task(task) for task in tasks))


async def get_proc_names_by_pattern(
    *,
    pool: asyncpg.Pool,
    schema: str,
    like: str,
) -> set[str]:
    async with pool.acquire(timeout=10) as con:
        result = await con.fetch(
            """
            SELECT
               p.proname as sp_name
            FROM pg_proc p
            LEFT JOIN pg_namespace n
               ON p.pronamespace = n.oid
            LEFT JOIN pg_language l
               ON p.prolang = l.oid
            LEFT JOIN pg_type t
               ON t.oid = p.prorettype
            WHERE
               n.nspname ILIKE $1
               AND p.prokind = 'p'
               AND p.proname ILIKE $2
            ORDER BY
                   sp_name
           """, schema, like
        )
        return {row[0] for row in result}


async def proc_failed(
    *,
    pool: asyncpg.Pool,
    proc_id: int,
    error_message: str,
    context: dict[str, typing.Hashable],
) -> None:
    async with pool.acquire(timeout=_DEFAULT_TIMEOUT) as con:
        await con.execute(
            "CALL dwr.proc_failed(p_proc_id := $1, p_error_message := $2, p_context := $3);",
            proc_id, error_message, context,
        )


async def proc_started(*, pool: asyncpg.Pool, batch_id: int, proc_name: str) -> int:
    async with pool.acquire(timeout=_DEFAULT_TIMEOUT) as con:
        return await con.fetchval(
            "SELECT * FROM dwr.proc_started(p_batch_id := $1, p_name := $2);",
            batch_id, proc_name,
        )


async def proc_succeeded(*, pool: asyncpg.Pool, proc_id: int, execution_millis: int) -> None:
    async with pool.acquire(timeout=_DEFAULT_TIMEOUT) as con:
        await con.execute(
            "CALL dwr.proc_succeeded(p_proc_id := $1, p_execution_millis := $2);",
            proc_id, execution_millis,
        )


async def run_proc(
    *,
    pool: asyncpg.Pool,
    schema: str,
    proc_name: str,
    incremental: bool,
) -> None:
    async with pool.acquire(timeout=10) as con:
        await con.execute(f"CALL {schema}.{proc_name}(p_incremental := $1);", incremental)
