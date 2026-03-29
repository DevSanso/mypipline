CREATE TABLE IF NOT EXISTS mypip_connection_info (
    identifier          TEXT
    id                         INT         NOT NULL,
    max_size                   BIGINT      NOT NULL,
    "name"                     TEXT        NOT NULL,
    conn_type                  TEXT        NOT NULL,
    conn_name                  TEXT        NOT NULL,
    conn_user                  TEXT        NOT NULL,
    conn_addr                  TEXT        NOT NULL,
    conn_passwd                TEXT        NOT NULL,
    conn_timeout               INTEGER     NOT NULL,

    odbc_driver                TEXT,
    odbc_current_time_query    TEXT,
    odbc_current_time_col_name TEXT,

    PRIMARY KEY(identifier, "name" )
);

CREATE TABLE IF NOT EXISTS mypip_plan_chain_bind_param (
    id         BIGINT      NOT NULL,
    chain_id        TEXT        NOT NULL,
    idx             BIGINT      NOT NULL,
    "key"             TEXT        NOT NULL,
    bind_id         TEXT        NOT NULL,
    "row"             BIGINT,

    PRIMARY KEY(id, chain_id)
);

CREATE TABLE IF NOT EXISTS mypip_plan_chain_args (
    id         BIGINT      NOT NULL,
    chain_id        TEXT        NOT NULL,
    "data"            TEXT        NOT NULL,
    idx             BIGINT      NOT NULL,

    PRIMARY KEY(id, chain_id)
);

CREATE TABLE IF NOT EXISTS mypip_plan_chain_mapping (
    chain_id TEXT,
    mapping_type TEXT,
    ranking INT,
    args_or_bind_id BIGINT,

    PRIMARY KEY(chain_id, ranking) 
);

CREATE UNIQUE INDEX mypip_plan_chain_mapping_u1 ON mypip_plan_chain_mapping(chain_id, args_or_bind_id);

CREATE TABLE IF NOT EXISTS mypip_plan_chain (
    id              TEXT         NOT NULL PRIMARY KEY,
    plan_id         INT         NOT NULL,
    next_chain_id        TEXT        NOT NULL,
    connection      TEXT        NOT NULL,
    query           TEXT        NOT NULL
);

CREATE INDEX mypip_plan_chain_i1 ON mypip_plan_chain(plan_id);
CREATE INDEX mypip_plan_chain_i2 ON mypip_plan_chain(next_chain_id);

CREATE TABLE IF NOT EXISTS mypip_plan_script (
    id              INT         NOT NULL PRIMARY KEY,
    plan_id         INT         NOT NULL,
    lang            TEXT        NOT NULL,
    "file"            TEXT        NOT NULL
);

CREATE UNIQUE INDEX mypip_plan_script_u1 ON mypip_plan_script(plan_id);

CREATE TABLE IF NOT EXISTS mypip_plan (
    identifier          TEXT
    id                  INT         NOT NULL PRIMARY KEY,
    "name"              TEXT        NOT NULL,
    type_name           TEXT        NOT NULL,
    "enable"              BOOLEAN     NOT NULL DEFAULT TRUE,
    interval_connection TEXT,
    interval_second     BIGINT      NOT NULL,

    PRIMARY KEY(identifier, id)
);

create table mypip_plan_script_data (
	identifier TEXT,
	script_file TEXT,
	script_data TEXT not null,

	primary key(identifier, script_file)
)