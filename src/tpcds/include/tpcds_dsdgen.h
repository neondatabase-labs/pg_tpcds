#pragma once

#include <filesystem>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

extern "C" {
#include "tpcds-kit/tools/config.h"
#include "tpcds-kit/tools/porting.h"
}

namespace tpcds {
class TPCDSTableGenerator {
 public:
  TPCDSTableGenerator(uint32_t scale_factor, const std::string& table, std::filesystem::path resource_dir,
                      int rng_seed = 19620718);

  bool generate_call_center() const;
  bool generate_catalog_page() const;
  bool generate_catalog_sales_and_returns() const;
  bool generate_customer_address() const;
  bool generate_customer() const;
  bool generate_customer_demographics() const;
  bool generate_date_dim() const;
  bool generate_household_demographics() const;
  bool generate_income_band() const;
  bool generate_inventory() const;
  bool generate_item() const;
  bool generate_promotion() const;
  bool generate_reason() const;
  bool generate_ship_mode() const;
  bool generate_store() const;
  bool generate_store_sales_and_returns() const;
  bool generate_time_dim() const;
  bool generate_warehouse() const;
  bool generate_web_page() const;
  bool generate_web_sales_and_returns() const;
  bool generate_web_site() const;

 private:
  uint32_t _scale_factor;
  std::string table_;
};

}  // namespace tpcds
