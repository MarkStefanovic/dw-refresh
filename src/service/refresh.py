import asyncpg
import loguru as loguru

from src.adapter import db

__all__ = ("refresh",)


async def refresh(*, incremental: bool, max_connections: int, schema: str, connection_string: str) -> None:
    pool = await asyncpg.create_pool(connection_string)
    await _run_procs_matching_pattern(pool=pool, schema=schema, incremental=incremental, max_connections=max_connections, like="refresh\_p\_%")
    await _run_procs_matching_pattern(pool=pool, schema=schema, incremental=incremental, max_connections=max_connections, like="refresh\_h\_%")
    await _run_procs_matching_pattern(pool=pool, schema=schema, incremental=incremental, max_connections=max_connections, like="refresh\_l\_%")
    await _run_procs_matching_pattern(pool=pool, schema=schema, incremental=incremental, max_connections=max_connections, like="refresh\_sal\_%")
    await _run_procs_matching_pattern(pool=pool, schema=schema, incremental=incremental, max_connections=max_connections, like="refresh\_s\_%")
    await _run_procs_matching_pattern(pool=pool, schema=schema, incremental=incremental, max_connections=max_connections, like="refresh\_%\_dim")
    await _run_procs_matching_pattern(pool=pool, schema=schema, incremental=incremental, max_connections=max_connections, like="refresh\_%\_fact")


async def _run_proc(*, pool: asyncpg.Pool, schema: str, stored_proc: str, incremental: bool) -> None:
    async with pool.acquire(timeout=10) as con:
        loguru.logger.info(f"Starting {schema}.{stored_proc}...")
        await con.execute(f"CALL {schema}.{stored_proc}(p_incremental := $1);", incremental)
        loguru.logger.info(f"{schema}.{stored_proc} completed successfully.")


async def _run_procs(*, pool: asyncpg.Pool, schema: str, stored_procs: set[str], incremental: bool, max_connections: int) -> None:
    tasks = [
        _run_proc(pool=pool, schema=schema, stored_proc=stored_proc, incremental=incremental)
        for stored_proc in stored_procs
    ]
    await db.gather_with_limited_concurrency(max_connections, *tasks)


async def _run_procs_matching_pattern(*, pool: asyncpg.Pool, schema: str, like: str, incremental: bool, max_connections: int) -> None:
    procs = await db.get_proc_names_by_pattern(pool=pool, schema=schema, like=like)
    await _run_procs(
        pool=pool,
        schema=schema,
        stored_procs=procs,
        incremental=incremental,
        max_connections=max_connections,
    )
