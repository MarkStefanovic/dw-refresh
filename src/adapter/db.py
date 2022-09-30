import asyncio
import json
import re
import typing

import asyncpg

__all__ = (
    "batch_failed",
    "batch_started",
    "batch_succeeded",
    "cleanup",
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
        await con.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )
        return await con.execute(
            "CALL dwr.batch_failed(p_batch_id := $1, p_error_message := $2, p_context := $3::JSONB);",
            batch_id, error_message, context,
        )


async def batch_started(*, pool: asyncpg.Pool, context: dict[str, typing.Hashable]) -> int:
    async with pool.acquire(timeout=_DEFAULT_TIMEOUT) as con:
        await con.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )
        return await con.fetchval(
            "SELECT * FROM dwr.batch_started(p_context := $1::JSONB) AS bid;",
            context,
        )


async def batch_succeeded(
    *,
    pool: asyncpg.Pool,
    batch_id: int,
    execution_millis: int,
    context: dict[str, typing.Hashable],
) -> int:
    async with pool.acquire(timeout=_DEFAULT_TIMEOUT) as con:
        await con.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )
        return await con.execute(
            "CALL dwr.batch_succeeded (p_batch_id := $1, p_execution_millis := $2, p_context := $3::JSONB);",
            batch_id, execution_millis, context,
        )


async def cleanup(*, pool: asyncpg.Pool, days_logs_to_keep: int) -> None:
    async with pool.acquire(timeout=_DEFAULT_TIMEOUT) as con:
        return await con.execute("CALL dwr.cleanup (p_days_to_keep := $1);", days_logs_to_keep)


async def gather_with_limited_concurrency(
    n: int,
    *tasks: asyncio.Task[typing.Coroutine[typing.Any, typing.Any, None]],
) -> tuple[asyncio.Future[typing.Any], ...]:
    semaphore = asyncio.Semaphore(n)

    async def wrap_task(task):
        async with semaphore:
            return await task

    return await asyncio.gather(*(wrap_task(task) for task in tasks))


async def get_proc_names_by_pattern(
    *,
    pool: asyncpg.Pool,
    schema: str,
    pattern: str,
) -> set[str]:
    async with pool.acquire(timeout=10) as con:
        result = await con.fetch(
            """
            SELECT DISTINCT
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
            """,
            schema,
        )
        proc_names = {row[0] for row in result}
        return {
            proc_name for proc_name in proc_names
            if re.match(pattern, proc_name, flags=re.IGNORECASE)
        }


async def proc_failed(
    *,
    pool: asyncpg.Pool,
    proc_id: int,
    error_message: str,
    context: dict[str, typing.Hashable],
) -> None:
    async with pool.acquire(timeout=_DEFAULT_TIMEOUT) as con:
        await con.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )
        await con.execute(
            "CALL dwr.proc_failed(p_proc_id := $1, p_error_message := $2, p_context := $3::JSONB);",
            proc_id, error_message, context,
        )


async def proc_started(
    *,
    pool: asyncpg.Pool,
    batch_id: int,
    proc_name: str,
    context: dict[str, typing.Hashable],
) -> int:
    async with pool.acquire(timeout=_DEFAULT_TIMEOUT) as con:
        await con.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )
        return await con.fetchval(
            "SELECT * FROM dwr.proc_started(p_batch_id := $1, p_proc_name := $2, p_context := $3::JSONB);",
            batch_id, proc_name, context,
        )


async def proc_succeeded(
    *,
    pool: asyncpg.Pool,
    proc_id: int,
    execution_millis: int,
    context: dict[str, typing.Hashable],
) -> None:
    async with pool.acquire(timeout=_DEFAULT_TIMEOUT) as con:
        await con.set_type_codec(
            "jsonb",
            encoder=json.dumps,
            decoder=json.loads,
            schema='pg_catalog'
        )
        await con.execute(
            "CALL dwr.proc_succeeded(p_proc_id := $1, p_execution_millis := $2, p_context := $3::JSONB);",
            proc_id, execution_millis, context,
        )


async def run_proc(
    *,
    pool: asyncpg.Pool,
    schema: str,
    proc_name: str,
    proc_args: dict[str, typing.Hashable],
) -> None:
    async with pool.acquire(timeout=10) as con:
        param_placeholders = ", ".join(
            f"{param} := ${i}"
            for i, param in enumerate(proc_args.keys(), start=1)
        )
        await con.execute(f"CALL {schema}.{proc_name}({param_placeholders});", *proc_args.values())
