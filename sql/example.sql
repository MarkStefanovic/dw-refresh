INSERT INTO dwr.batch (batch_id, ts)
OVERRIDING SYSTEM VALUE
VALUES
    (1, '2020-01-01 +0')
,   (2, now())
ON CONFLICT DO NOTHING
;
INSERT INTO dwr.batch_success (batch_id, execution_millis)
OVERRIDING SYSTEM VALUE
VALUES
    (1, 5000)
,   (2, 7000)
ON CONFLICT DO NOTHING
;
INSERT INTO dwr.proc (proc_id, batch_id, proc_name, context, ts)
OVERRIDING SYSTEM VALUE
VALUES
    (1, 1, 'test_proc', jsonb_build_object(), '2020-01-02 01:00 +0')
,   (2, 2, 'test_proc', jsonb_build_object(), '2020-01-02 02:00 +0')
,   (3, 2, 'test_proc_2', jsonb_build_object(), '2020-01-02 02:00 +0')
ON CONFLICT DO NOTHING
;
INSERT INTO dwr.proc_success (proc_id, execution_millis, ts)
OVERRIDING SYSTEM VALUE
VALUES
    (1, 200, '2020-01-02 01:00 +0')
,   (2, 300, '2020-01-02 03:00 +0')
ON CONFLICT DO NOTHING
;

INSERT INTO dwr.proc_failure (proc_id, error_message, ts)
OVERRIDING SYSTEM VALUE
VALUES
    (3, 'Whoops!', '2020-01-02 04:00 +0')
ON CONFLICT DO NOTHING
;

SELECT
    lpr.proc_name
,   lpr.error_message
,   lpr.result
,   lpr.start_ts AT TIME ZONE 'America/Los_Angeles' AS start_ts
,   lpr.end_ts AT TIME ZONE 'America/Los_Angeles' AS end_ts
,   lpr.execution_seconds
,   lpr.context
FROM dwr.get_latest_proc_results() AS lpr;

SELECT * FROM dwr.batch;
SELECT * FROM dwr.proc;

CALL dwr.cleanup(p_days_to_keep := 3);
