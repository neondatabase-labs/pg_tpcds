## TPC-DS FOR POSTGRES

A plug-and-play postgres extension for testing tpcds, inspired by [duckdb](https://github.com/duckdb/duckdb.git) and [hyrise](https://github.com/hyrise/hyrise.git), to avoid the cumbersome configuration in tpcds. Only need to install the extension, and then you can run the test.

### REQUIREMENTS

- PostgreSQL
    - tested only pg 17 currently, need more test
- gcc 13 or higher

### SETUP

make sure you have installed the postgres and the `pg_config` is in your PATH.

```
git clone https://github.com/asky/pg_tpcds.git
git submodule update
cd pg_tpcds
cmake -Bbuild
cmake --build build --target install
```

### BASIC USAGE

```sql
-- create extension pg_tpcds;
create extension pg_tpcds;

-- generate data set for scale factor 1
select * from dsdgen(1);
          tab           | row_count
------------------------+-----------
 call_center            |         6
 catalog_page           |     11718
 catalog_sales          |   1441548
 customer               |    100000
 customer_address       |     50000
 customer_demographics  |   1920800
 date_dim               |     73049
 household_demographics |      7200
 income_band            |        20
 inventory              |  11745000
 item                   |     18000
 promotion              |       300
 reason                 |        35
 ship_mode              |        20
 store                  |        12
 store_sales            |   2880404
 time_dim               |     86400
 warehouse              |         5
 web_page               |        60
 web_sales              |    719384
 web_site               |        30
(21 rows)

Time: 55490.768 ms (00:55.491)

-- get all queries
select * from tpcds_queries();
-- get query by id, from 1 to 99
select query from tpcds_queries(1); \gset

-- and exec it
:query

-- cleanup all tables
select tpcds_cleanup();

-- drop extension pg_tpcds will auto remove all tables and functions
drop extension pg_tpcds;

```

### CUSTOMIZATION

* 99 SQL statements have been provided here in advance. you can modify the SQL statement before installing the extension,  the file located at `src/tpcds/queries`.
* Standard table creation statements are provided here for your configuration. The file is located at `src/tpcds/schema`.
   * You can directly modify the table creation statements, such as specifying primary keys.
   * Alternatively, you can configure index creation statements in the file `src/post_prepare.sql`, which will be invoked after table creation





