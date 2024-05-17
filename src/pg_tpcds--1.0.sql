-- generate tpcds dateset
--  args:
--   scale factor in GB: 1, 10, 100, 300, 1000, 3000, 10000
--   overwrite: true or false
--   
-- CREATE FUNCTION dsdgen(IN double sf,
--                        IN overwrite boolean,
--                        IN text table DEFAULT '')
--     RETURNS boolean
--     AS 'MODULE_PATHNAME', 'dsdgen'
--     LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tpcds_prepare() RETURNS BOOLEAN AS 'MODULE_PATHNAME',
'tpcds_prepare' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tpcds_cleanup() RETURNS BOOLEAN AS 'MODULE_PATHNAME',
'tpcds_cleanup' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tpcds_queries(IN QID INT DEFAULT 0, OUT qid INT, OUT query TEXT) RETURNS SETOF record AS 'MODULE_PATHNAME',
'tpcds_queries' LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tpcds_run(
  IN QID INT DEFAULT 0,
  OUT id INT,
  OUT duration DOUBLE PRECISION,
  OUT Checked BOOLEAN
) RETURNS record AS 'MODULE_PATHNAME',
'tpcds_runner' LANGUAGE C IMMUTABLE STRICT;


CREATE FUNCTION tpcds_run_internal(IN QID INT, IN REPLACE BOOLEAN DEFAULT false) RETURNS TABLE (
  ret_id INT,
  new_duration DOUBLE PRECISION,
  old_duration DOUBLE PRECISION,
  ret_checked BOOLEAN
) AS $$
DECLARE
    run_record record;
BEGIN
    SELECT id, duration, checked INTO run_record FROM tpcds_run(QID);

    ret_id := run_record.id;
    new_duration := run_record.duration;
    ret_checked := run_record.checked;

    SELECT ec_duration INTO old_duration FROM tpcds.tpcds_query_stats WHERE ec_qid = run_record.id;

    IF NOT FOUND THEN
        INSERT INTO tpcds.tpcds_query_stats VALUES (run_record.id, run_record.duration, current_timestamp(6));
    ELSE
        IF REPLACE AND run_record.duration < old_duration THEN
            UPDATE tpcds.tpcds_query_stats
            SET ec_duration = run_record.duration, ec_recoed_time = current_timestamp(6)
            WHERE ec_qid = run_record.id;
        END IF;
    END IF;

    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;


CREATE FUNCTION tpcds(VARIADIC queries INT [] DEFAULT '{0}' :: INT []) RETURNS TABLE (
  "Qid" CHAR(2),
  "Stable(ms)" TEXT,
  "Current(ms)" TEXT,
  "Diff(%)" TEXT,
  "Result" TEXT
) AS $$
DECLARE
    run_record record;
    sum_statble numeric := 0;
    sum_run numeric := 0;
    query_count int := array_length(queries, 1);
    run_all boolean := false;
    i int;
BEGIN
    IF query_count = 1 AND queries[1] = 0 THEN
        run_all := true;
        query_count := 99;
    END IF;

    FOR i IN 1..query_count LOOP
        IF queries[i] = 0 AND NOT run_all THEN
            continue;
        END IF;

        IF run_all THEN
            SELECT ret_id, new_duration, old_duration, ret_checked INTO run_record FROM tpcds_run_internal(i, true);
        ELSE 
            SELECT ret_id, new_duration, old_duration, ret_checked INTO run_record FROM tpcds_run_internal(queries[i]);
        END IF;

        "Qid" := to_char(run_record.ret_id, '09');
        "Current(ms)" := to_char(run_record.new_duration, '9999990.99');
        "Stable(ms)" := to_char(run_record.old_duration, '9999990.99');
        "Diff(%)" := to_char((run_record.new_duration - run_record.old_duration) / run_record.old_duration * 100, 's990.99');
        "Result" := (run_record.ret_checked)::text;

        sum_run := sum_run + run_record.new_duration;
        sum_statble := sum_statble + run_record.old_duration;

        RETURN NEXT;
    END LOOP;
    "Qid" := '----';
    "Stable(ms)" := '-----------';
    "Current(ms)" := '-----------';
    "Diff(%)" := '-------';
    "Result" := '';
    RETURN NEXT;

    "Qid" := 'Sum';
    "Stable(ms)" := to_char(sum_statble, '9999990.99');
    "Current(ms)" := to_char(sum_run, '9999990.99');
    "Diff(%)" := to_char((sum_run - sum_statble) / sum_statble * 100, 's990.99');
    "Result" := '';
    RETURN NEXT;
END;
$$ LANGUAGE plpgsql;


create view tpcds as select * from tpcds();

select tpcds_cleanup();

select tpcds_prepare();