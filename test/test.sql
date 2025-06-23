-- duckdb
-- EXPORT DATABASE '~/tpcds/sf1' (FORMAT CSV);
-- \i ~/tpcds/sf1/schema.sql
-- \i ~/tpcds/sf1/load.sql


create or replace function table_diff(tablex text) returns int as $$
declare
  cnt int;
begin
  execute format('select count(*) from (select * from %I except select * from %I)', 'dk_' || tablex, tablex) into cnt;
  return cnt;
end;
$$ language plpgsql;

select tpcds_cleanup();

-- select * from dsdgen_internal(1, 'call_center');
-- select * from dsdgen_internal(1, 'catalog_page');
-- select * from dsdgen_internal(1, 'customer_address');
-- select * from dsdgen_internal(1, 'customer');
-- select * from dsdgen_internal(1, 'customer_demographics');
-- select * from dsdgen_internal(1, 'date_dim');
-- select * from dsdgen_internal(1, 'household_demographics');
-- select * from dsdgen_internal(1, 'income_band');
-- select * from dsdgen_internal(1, 'inventory');
-- select * from dsdgen_internal(1, 'item');
-- select * from dsdgen_internal(1, 'promotion');
-- select * from dsdgen_internal(1, 'reason');
-- select * from dsdgen_internal(1, 'ship_mode');
-- select * from dsdgen_internal(1, 'store');
-- select * from dsdgen_internal(1, 'time_dim');
-- select * from dsdgen_internal(1, 'warehouse');
-- select * from dsdgen_internal(1, 'web_page');
-- select * from dsdgen_internal(1, 'web_site');
-- select * from dsdgen_internal(1, 'web_sales');
-- select * from dsdgen_internal(1, 'store_sales');
-- select * from dsdgen_internal(1, 'catalog_sales');

select * from dsdgen(1);


select * from table_diff('call_center');
select * from table_diff('catalog_page');
select * from table_diff('customer_address');
select * from table_diff('customer');
select * from table_diff('customer_demographics');
select * from table_diff('date_dim');
select * from table_diff('household_demographics');
select * from table_diff('income_band');
select * from table_diff('inventory');
select * from table_diff('item');
select * from table_diff('promotion');
select * from table_diff('reason');
select * from table_diff('ship_mode');
select * from table_diff('store');
select * from table_diff('time_dim');
select * from table_diff('warehouse');
select * from table_diff('web_page');
select * from table_diff('web_site');
select * from table_diff('web_sales');
select * from table_diff('web_returns');
select * from table_diff('store_sales');
select * from table_diff('store_returns');
select * from table_diff('catalog_sales');
select * from table_diff('catalog_returns');


