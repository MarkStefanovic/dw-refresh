import asyncio

import asyncpg

__all__ = ("gather_with_limited_concurrency", "get_proc_names_by_pattern")


async def gather_with_limited_concurrency(n: int, *tasks):
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
