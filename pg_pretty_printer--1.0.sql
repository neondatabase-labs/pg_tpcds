\echo Use "CREATE EXTENSION pg_pretty_printer" to load this file. \quit
CREATE FUNCTION pg_pretty_printer(int, CHAR) RETURNS INTEGER
AS '$libdir/pg_pretty_printer'
LANGUAGE C IMMUTABLE STRICT