-- indexes
CREATE INDEX cs_item_order_number_idx ON catalog_sales ( cs_item_sk, cs_order_number);

CREATE INDEX cr_item_order_number_idx ON catalog_returns ( cr_item_sk, cr_order_number);

CREATE INDEX inv_date_item_warehouse_cluidx ON inventory ( inv_date_sk, inv_item_sk, inv_warehouse_sk );

CREATE INDEX ss_item_ticket_number_idx ON store_sales (ss_item_sk, ss_ticket_number);

CREATE INDEX sr_item_ticket_number_idx ON store_returns (sr_item_sk, sr_ticket_number);

CREATE INDEX ws_item_order_number_idx ON web_sales ( ws_item_sk, ws_order_number);

CREATE INDEX wr_item_order_number_idx ON web_returns ( wr_item_sk, wr_order_number);

-- additional indexes for performance
CREATE INDEX cs_sold_date_sk_cluidx ON catalog_sales(cs_sold_date_sk); 

CREATE INDEX cr_returned_date_cluidx ON catalog_returns(cr_returned_date_sk); 

CREATE INDEX ss_sold_date_sk_cluidx ON store_sales(ss_sold_date_sk); 

CREATE INDEX sr_returned_date_cluidx ON store_returns(sr_returned_date_sk); 

CREATE INDEX ws_sold_date_sk_cluidx ON web_sales(ws_sold_date_sk); 

CREATE INDEX wr_returnd_date_cluidx ON web_returns(wr_returned_date_sk); 