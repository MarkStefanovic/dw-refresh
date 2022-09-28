/*
DROP SCHEMA dwr CASCADE;
 */
CREATE SCHEMA IF NOT EXISTS dwr;

DROP TABLE IF EXISTS
    dwr.batch
,   dwr.batch_failure
,   dwr.batch_success
,   dwr.proc
,   dwr.proc_failure
,   dwr.proc_success
;

DROP PROCEDURE IF EXISTS dwr.batch_failed;
DROP FUNCTION IF EXISTS dwr.batch_started;
DROP PROCEDURE IF EXISTS dwr.batch_succeeded;
DROP PROCEDURE IF EXISTS dwr.proc_failed;
DROP FUNCTION IF EXISTS dwr.proc_started;
DROP PROCEDURE IF EXISTS dwr.proc_succeeded;
DROP FUNCTION IF EXISTS dwr.get_latest_proc_results;

CREATE TABLE IF NOT EXISTS dwr.batch (
    batch_id SERIAL PRIMARY KEY
,   context JSONB NOT NULL DEFAULT jsonb_build_object()
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwr.batch_failure (
    batch_id INT PRIMARY KEY REFERENCES dwr.batch (batch_id) ON DELETE CASCADE
,   error_message TEXT NOT NULL
,   context JSONB NOT NULL DEFAULT jsonb_build_object()
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwr.batch_success (
    batch_id INT PRIMARY KEY REFERENCES dwr.batch (batch_id) ON DELETE CASCADE
,   execution_millis INT NOT NULL CHECK (execution_millis >= 0)
,   context JSONB NOT NULL DEFAULT jsonb_build_object()
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwr.proc (
    proc_id SERIAL PRIMARY KEY
,   batch_id INT NOT NULL REFERENCES dwr.batch (batch_id) ON DELETE CASCADE
,   proc_name TEXT NOT NULL
,   context JSONB NOT NULL DEFAULT jsonb_build_object()
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwr.proc_failure (
    proc_id INT PRIMARY KEY REFERENCES dwr.proc (proc_id) ON DELETE CASCADE
,   error_message TEXT NOT NULL
,   context JSONB NOT NULL DEFAULT jsonb_build_object()
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS dwr.proc_success (
    proc_id INT PRIMARY KEY REFERENCES dwr.proc (proc_id) ON DELETE CASCADE
,   execution_millis INT NOT NULL CHECK (execution_millis >= 0)
,   context JSONB NOT NULL DEFAULT jsonb_build_object()
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);

CREATE OR REPLACE FUNCTION dwr.batch_started (
    p_context JSONB
)
RETURNS INT
LANGUAGE sql
AS $$
    INSERT INTO dwr.batch (context)
    VALUES (p_context)
    RETURNING batch_id;
$$;

CREATE OR REPLACE PROCEDURE dwr.batch_failed (
    p_batch_id INT
,   p_error_message TEXT
,   p_context JSONB
)
LANGUAGE sql
AS $$
    INSERT INTO dwr.batch_failure (batch_id, error_message, context)
    VALUES (p_batch_id, p_error_message, p_context);
$$;

CREATE OR REPLACE PROCEDURE dwr.batch_succeeded (
    p_batch_id INT
,   p_execution_millis INT
,   p_context JSONB
)
LANGUAGE sql
AS $$
    INSERT INTO dwr.batch_success (batch_id, execution_millis, context)
    VALUES (p_batch_id, p_execution_millis, p_context);
$$;

CREATE OR REPLACE FUNCTION dwr.proc_started (
    p_batch_id INT
,   p_proc_name TEXT
,   p_context JSONB
)
RETURNS INT
LANGUAGE sql
AS $$
    INSERT INTO dwr.proc (batch_id, proc_name, context)
    VALUES (p_batch_id, p_proc_name, p_context)
    RETURNING proc_id;
$$;

CREATE OR REPLACE PROCEDURE dwr.proc_failed (
    p_proc_id INT
,   p_error_message TEXT
,   p_context JSONB
)
LANGUAGE sql
AS $$
    INSERT INTO dwr.proc_failure (proc_id, error_message, context)
    VALUES (p_proc_id, p_error_message, p_context);
$$;

CREATE OR REPLACE PROCEDURE dwr.proc_succeeded (
    p_proc_id INT
,   p_execution_millis INT
,   p_context JSONB
)
LANGUAGE sql
AS $$
    INSERT INTO dwr.proc_success (proc_id, execution_millis, context)
    VALUES (p_proc_id, p_execution_millis, p_context);
$$;

CREATE OR REPLACE PROCEDURE dwr.cleanup (p_days_to_keep INT)
LANGUAGE sql
AS $$
    DELETE FROM dwr.batch AS b
    WHERE b.ts < now() - (p_days_to_keep || ' DAYS')::INTERVAL;
$$;

CREATE OR REPLACE FUNCTION dwr.get_latest_proc_results ()
RETURNS TABLE (
    proc_name TEXT
,   error_message TEXT
,   result TEXT
,   start_ts TIMESTAMPTZ(0)
,   end_ts TIMESTAMPTZ(0)
,   execution_seconds INT
,   context JSONB
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_latest_batch_id INT;
BEGIN
    v_latest_batch_id = (
        SELECT b.batch_id
        FROM dwr.batch AS b
        ORDER BY b.ts DESC
        LIMIT 1
    );

    RETURN QUERY
    WITH latest_procs AS (
        SELECT
            p.proc_id
        ,   p.proc_name
        ,   p.ts::TIMESTAMPTZ(0) AS start_ts
        FROM dwr.proc AS p
        WHERE
            p.batch_id = v_latest_batch_id
    )
    , successes AS (
        SELECT
            s.proc_id
        ,   s.context
        ,   s.execution_millis
        ,   s.ts AS end_ts
        FROM dwr.proc_success AS s
        JOIN latest_procs AS lp
            ON s.proc_id = lp.proc_id
    )
    , failures AS (
        SELECT
            f.proc_id
        ,   f.context
        ,   f.error_message
        ,   f.ts AS end_ts
        FROM dwr.proc_failure AS f
        JOIN latest_procs AS lp
            ON f.proc_id = lp.proc_id
    )
    SELECT
        lp.proc_name
    ,   f.error_message
    ,   CASE
            WHEN s.proc_id IS NULL AND f.proc_id IS NULL THEN ''
            WHEN s.proc_id IS NULL THEN 'fail'
            WHEN f.proc_id IS NULL THEN 'success'
        END AS result
    ,   lp.start_ts
    ,   coalesce(s.end_ts, f.end_ts)::TIMESTAMPTZ(0) AS end_ts
    ,   extract(EPOCH FROM (coalesce(s.end_ts, f.end_ts) - lp.start_ts))::INT AS execution_seconds
    ,   coalesce(s.context, f.context) AS context
    FROM latest_procs AS lp
    LEFT JOIN successes AS s
        ON lp.proc_id = s.proc_id
    LEFT JOIN failures AS f
        ON lp.proc_id = f.proc_id
    ORDER BY
        lp.proc_name
    ;
END;
$$;
