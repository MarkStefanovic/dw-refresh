import asyncpg
import loguru as loguru

from src.adapter import db

__all__ = ("check",)


async def check(*, max_connections: int, schema: str, connection_string: str) -> None:
    pool = await asyncpg.create_pool(connection_string)
    await _run_procs_matching_pattern(pool=pool, schema=schema, max_connections=max_connections, like="check_refresh\_%")


async def _run_proc(*, pool: asyncpg.Pool, schema: str, stored_proc: str) -> None:
    async with pool.acquire(timeout=10) as con:
        loguru.logger.info(f"Starting {schema}.{stored_proc}...")
        await con.execute(f"CALL {schema}.{stored_proc}();")
        loguru.logger.info(f"{schema}.{stored_proc} completed successfully.")


async def _run_procs(*, pool: asyncpg.Pool, schema: str, stored_procs: set[str], max_connections: int) -> None:
    tasks = [
        _run_proc(pool=pool, schema=schema, stored_proc=stored_proc)
        for stored_proc in stored_procs
    ]
    await db.gather_with_limited_concurrency(max_connections, *tasks)


async def _run_procs_matching_pattern(*, pool: asyncpg.Pool, schema: str, like: str, max_connections: int) -> None:
    procs = await db.get_proc_names_by_pattern(pool=pool, schema=schema, like=like)
    await _run_procs(pool=pool, schema=schema, stored_procs=procs, max_connections=max_connections)
