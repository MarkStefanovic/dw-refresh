CREATE SCHEMA dwr;

CREATE TABLE dwr.batch (
    batch_id SERIAL PRIMARY KEY
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE dwr.batch_failure (
    batch_id INT PRIMARY KEY REFERENCES dwr.batch (batch_id)
,   error_message TEXT NOT NULL
,   context JSONB NOT NULL DEFAULT jsonb_build_object()
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE dwr.batch_success (
    batch_id INT PRIMARY KEY REFERENCES dwr.batch (batch_id)
,   execution_millis INT NOT NULL CHECK (execution_millis >= 0)
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE dwr.proc (
    proc_id SERIAL PRIMARY KEY
,   batch_id INT NOT NULL REFERENCES dwr.batch (batch_id)
,   proc_name TEXT NOT NULL
,   ts TIMESTAMPTZ(3) NOT NULL DEFAULT now()
);

CREATE TABLE dwr.proc_failure (
    proc_id INT PRIMARY KEY REFERENCES dwr.proc (proc_id)
,   error_message TEXT NOT NULL
,   context JSONB NOT NULL DEFAULT jsonb_build_object()
);

CREATE TABLE dwr.proc_success (
    proc_id INT PRIMARY KEY REFERENCES dwr.proc (proc_id)
,   execution_millis INT NOT NULL CHECK (execution_millis >= 0)
,   ts TIMESTAMPTZ(0) NOT NULL DEFAULT now()
);

