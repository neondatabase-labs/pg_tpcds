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

CREATE FUNCTION tpcds_prepare()
    RETURNS boolean
    AS 'MODULE_PATHNAME', 'tpcds_prepare'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tpcds_cleanup()
    RETURNS boolean
    AS 'MODULE_PATHNAME', 'tpcds_cleanup'
    LANGUAGE C IMMUTABLE STRICT;

CREATE FUNCTION tpcds_queries(IN QID int DEFAULT 0,
    OUT qid INT,
    OUT query text)
    RETURNS SETOF record
    AS 'MODULE_PATHNAME', 'tpcds_queries'
    LANGUAGE C IMMUTABLE STRICT;


CREATE FUNCTION tpcds_run(IN QID int DEFAULT 0,
    OUT id int,
    OUT duration double precision, -- in ms
    OUT Checked boolean)
    RETURNS SETOF record
    AS 'MODULE_PATHNAME', 'tpcds_runner'
    LANGUAGE C IMMUTABLE STRICT;


-- TODO: variadic parameters
CREATE FUNCTION tpcds() RETURNS TABLE (
    "Qid" char(2),
    "Stable(ms)" text,
    "Current(ms)" text,
    "Diff(%)" text,
    "Result" text
) AS $$
DECLARE
    run_record record;
    old_ec_duration numeric;
    sum_statble numeric := 0;
    sum_run numeric := 0;
BEGIN
    FOR run_record IN SELECT id, duration, Checked FROM tpcds_run(0) LOOP
        "Qid" := to_char(run_record.id, '09');
        "Current(ms)" := to_char(run_record.duration, '9999990.99');

        sum_run := sum_run + run_record.duration;

        SELECT ec_duration, sum_statble + ec_duration INTO old_ec_duration,  sum_statble FROM tpcds.tpcds_query_stats WHERE ec_qid = run_record.id;

        IF NOT FOUND THEN
            INSERT INTO tpcds.tpcds_query_stats VALUES (run_record.id, run_record.duration, current_timestamp(6));
        ELSE
            IF run_record.duration < old_ec_duration THEN
                UPDATE tpcds.tpcds_query_stats
                SET ec_duration = run_record.duration, ec_recoed_time = current_timestamp(6)
                WHERE ec_qid = run_record.id;
            END IF;
        END IF;

        SELECT to_char(old_ec_duration, '9999990.99') INTO "Stable(ms)";

        "Diff(%)" := to_char((run_record.duration - old_ec_duration) / old_ec_duration * 100, 's990.99');

        "Result" := (run_record.Checked)::text;

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