-- generate tpcds dateset
--  args:
--   scale factor in GB: 1, 10, 100, 300, 1000, 3000, 10000
--   overwrite: true or false
-- CREATE FUNCTION dsdgen(double, boolean)
--     RETURNS boolean
--     AS 'MODULE_PATHNAME', 'dsdgen'
--     LANGUAGE C IMMUTABLE STRICT;

-- -- create tpcds tables
-- --  args:
-- --   overwrite: true or false
-- CREATE FUNCTION tpcds_prepare(boolean)
--     RETURNS boolean
--     AS 'MODULE_PATHNAME', 'tpcds_prepare'
--     LANGUAGE C IMMUTABLE STRICT;

-- -- destroy tpcds tables
-- -- create tpcds tables
-- CREATE FUNCTION tpcds_destroy(pg_tpcds)
--     RETURNS bytea
--     AS 'MODULE_PATHNAME', 'tpcds_destroy'
--     LANGUAGE C IMMUTABLE STRICT;

-- get tpcds queries
-- create tpcds tables
--  args:
--   query num
CREATE FUNCTION tpcds_queries(IN QID int DEFAULT 0,
    OUT qid INT,
    OUT query text)
    RETURNS SETOF record
    AS 'MODULE_PATHNAME', 'tpcds_queries'
    LANGUAGE C IMMUTABLE STRICT;


-- run tpcds queries
-- create tpcds tables
--  args:
--   query num
-- CREATE FUNCTION tpcds(int)
--     RETURNS record
--     AS 'MODULE_PATHNAME', 'tpcds'
--     LANGUAGE C IMMUTABLE STRICT;