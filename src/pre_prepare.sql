drop schema if exists tpcds cascade;
create schema tpcds;

create table tpcds.tpcds_query_stats(
  ec_qid int ,
  ec_duration double precision,
  ec_recoed_time timestamp
);
