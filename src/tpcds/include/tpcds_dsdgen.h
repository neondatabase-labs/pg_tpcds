#pragma once

#include <string>
#include "porting.h"

namespace tpcds {

struct tpcds_table_def {
  const char* name;
  int table_id;
  int fl_small;
  int fl_child;
  int first_column;
};

class TPCDSTableGenerator {
 public:
  TPCDSTableGenerator(double scale_factor, const std::string& table);

  std::tuple<ds_key_t, ds_key_t> prepare_loader();

  int generate_call_center();
  int generate_catalog_page();
  int generate_catalog_sales_and_returns();
  int generate_customer_address();
  int generate_customer();
  int generate_customer_demographics();
  int generate_date_dim();
  int generate_household_demographics();
  int generate_income_band();
  int generate_inventory();
  int generate_item();
  int generate_promotion();
  int generate_reason();
  int generate_ship_mode();
  int generate_store();
  int generate_store_sales_and_returns();
  int generate_time_dim();
  int generate_warehouse();
  int generate_web_page();
  int generate_web_sales_and_returns();
  int generate_web_site();

 private:
  double _scale_factor;
  tpcds_table_def table_def_;
};

}  // namespace tpcds
