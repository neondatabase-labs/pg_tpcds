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
 catalog_returns        |   1441548
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
 store_returns          |   2880404
 time_dim               |     86400
 warehouse              |         5
 web_page               |        60
 web_sales              |    719384
 web_returns            |    719384
 web_site               |        30
(24 rows)

Time: 95856.793 ms (01:35.857)

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


### anwser

NEED ?

### TODO

- [ ] check query result correctness
- [ ] support postgres 12 or higher
- [ ] async dsdgen, need provide a async function to generate data set, or use dblink
    - Avoid relying on other extensions, we can do it ourselves.


```sql
EXPORT DATABASE '/home/asky/xx/sf1' (FORMAT CSV);



create or replace function table_diff(tablex text) returns int as $$
declare
  cnt int;
begin
  execute format('select count(*) from (select * from %I except select * from %I)', 'cc_' || tablex, tablex) into cnt;
  return cnt;
end;
$$ language plpgsql;

select tpcds_cleanup();

select * from dsdgen_internal(1, 'call_center', -1);
select * from table_diff('call_center');
select * from dsdgen_internal(1, 'catalog_page', -1);
select * from table_diff('catalog_page');
select * from dsdgen_internal(1, 'customer_address', -1);
select * from table_diff('customer_address');
select * from dsdgen_internal(1, 'customer', -1);
select * from table_diff('customer');
select * from dsdgen_internal(1, 'customer_demographics', -1);
select * from table_diff('customer_demographics');
select * from dsdgen_internal(1, 'date_dim', -1);
select * from table_diff('date_dim');
select * from dsdgen_internal(1, 'household_demographics', -1);
select * from table_diff('household_demographics');
select * from dsdgen_internal(1, 'income_band', -1);
select * from table_diff('income_band');
select * from dsdgen_internal(1, 'inventory', -1);
select * from table_diff('inventory');
select * from dsdgen_internal(1, 'item', -1);
select * from table_diff('item');
select * from dsdgen_internal(1, 'promotion', -1);
select * from table_diff('promotion');
select * from dsdgen_internal(1, 'reason', -1);
select * from table_diff('reason');
select * from dsdgen_internal(1, 'ship_mode', -1);
select * from table_diff('ship_mode');
select * from dsdgen_internal(1, 'store', -1); -- error
select * from table_diff('store');
select * from dsdgen_internal(1, 'time_dim', -1);
select * from table_diff('time_dim');
select * from dsdgen_internal(1, 'warehouse', -1);
select * from table_diff('warehouse');
select * from dsdgen_internal(1, 'web_page', -1);
select * from table_diff('web_page');
select * from dsdgen_internal(1, 'web_site', -1);
select * from table_diff('web_site');

select * from dsdgen_internal(1, 'web_sales', -1);
select * from table_diff('web_sales');
select * from table_diff('web_returns');
select * from dsdgen_internal(1, 'store_sales', -1);  -- error
select * from table_diff('store_sales');
select * from table_diff('store_returns');
select * from dsdgen_internal(1, 'catalog_sales', -1);
select * from table_diff('catalog_sales');
select * from table_diff('catalog_returns');




```
