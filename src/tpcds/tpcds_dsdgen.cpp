#include "tpcds_dsdgen.h"

#include <algorithm>
#include <climits>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <format>
#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

extern "C" {
#include "tpcds-kit/tools/address.h"
#include "tpcds-kit/tools/columns.h"
#include "tpcds-kit/tools/date.h"
#include "tpcds-kit/tools/decimal.h"
#include "tpcds-kit/tools/dist.h"
#include "tpcds-kit/tools/genrand.h"
#include "tpcds-kit/tools/nulls.h"
#include "tpcds-kit/tools/parallel.h"
#include "tpcds-kit/tools/porting.h"
#include "tpcds-kit/tools/r_params.h"
#include "tpcds-kit/tools/tables.h"
#include "tpcds-kit/tools/tdefs.h"
#include "tpcds-kit/tools/w_call_center.h"
#include "tpcds-kit/tools/w_catalog_page.h"
#include "tpcds-kit/tools/w_catalog_returns.h"
#include "tpcds-kit/tools/w_catalog_sales.h"
#include "tpcds-kit/tools/w_customer.h"
#include "tpcds-kit/tools/w_customer_address.h"
#include "tpcds-kit/tools/w_customer_demographics.h"
#include "tpcds-kit/tools/w_datetbl.h"
#include "tpcds-kit/tools/w_household_demographics.h"
#include "tpcds-kit/tools/w_income_band.h"
#include "tpcds-kit/tools/w_inventory.h"
#include "tpcds-kit/tools/w_item.h"
#include "tpcds-kit/tools/w_promotion.h"
#include "tpcds-kit/tools/w_reason.h"
#include "tpcds-kit/tools/w_ship_mode.h"
#include "tpcds-kit/tools/w_store.h"
#include "tpcds-kit/tools/w_store_returns.h"
#include "tpcds-kit/tools/w_store_sales.h"
#include "tpcds-kit/tools/w_timetbl.h"
#include "tpcds-kit/tools/w_warehouse.h"
#include "tpcds-kit/tools/w_web_page.h"
#include "tpcds-kit/tools/w_web_returns.h"
#include "tpcds-kit/tools/w_web_sales.h"
#include "tpcds-kit/tools/w_web_site.h"
}

namespace tpcds {
void init_tpcds_tools(uint32_t scale_factor, std::string resource_dir,
                      int rng_seed) {
  // setting some values that were intended by dsdgen to be passed via command
  // line

  auto scale_factor_string = std::string{"SCALE"};
  auto scale_factor_value_string = std::to_string(scale_factor);
  set_int(scale_factor_string.data(), scale_factor_value_string.data());

  auto rng_seed_string = std::string{"RNGSEED"};
  auto rng_seed_value_string = std::to_string(rng_seed);
  set_int(rng_seed_string.data(), rng_seed_value_string.data());

  // init_rand from genrand.c, adapted
  {
    const auto n_seed = get_int(rng_seed_string.data());
    const auto skip = INT_MAX / MAX_COLUMN;
    for (auto index = 0; index < MAX_COLUMN; ++index) {
      const auto seed = n_seed + skip * index;
      Streams[index].nInitialSeed = seed;
      Streams[index].nSeed = seed;
      Streams[index].nUsed = 0;
    }
  }

  mk_w_store_sales_master(nullptr, 0, 1);
  mk_w_web_sales_master(nullptr, 0, 1);
  mk_w_web_sales_detail(nullptr, 0, nullptr, nullptr, 1);
  mk_w_catalog_sales_master(nullptr, 0, 1);

  auto distributions_string = std::string{"DISTRIBUTIONS"};
  auto distributions_value = resource_dir + "/tpcds.idx";
  set_str(distributions_string.data(), distributions_value.data());

  for (auto table_id = int{0}; table_id <= MAX_TABLE; ++table_id) {
    resetSeeds(table_id);
    RNGReset(table_id);
  }
}

template <class TpcdsRow, int builder(void*, ds_key_t), int table_id>
TpcdsRow call_dbgen_mk(ds_key_t index) {
  auto tpcds_row = TpcdsRow{};
  builder(&tpcds_row, index);
  tpcds_row_stop(table_id);
  return tpcds_row;
}

// get starting index and row count for a table, see
// third_party/tpcds-kit/tools/driver.c:549
std::pair<ds_key_t, ds_key_t> prepare_for_table(int table_id) {
  auto k_row_count = ds_key_t{};
  auto k_first_row = ds_key_t{};

  split_work(table_id, &k_first_row, &k_row_count);

  const auto& tdefs = *getSimpleTdefsByNumber(table_id);

  if (k_first_row != 1) {
    row_skip(table_id, static_cast<int>(k_first_row - 1));
    if (tdefs.flags & FL_PARENT) {  // NOLINT
      row_skip(tdefs.nParam, static_cast<int>(k_first_row - 1));
    }
  }

  if (tdefs.flags & FL_SMALL) {  // NOLINT
    resetCountCount();
  }

  // Assert(k_row_count <= std::numeric_limits<tpcds_key_t>::max(),
  //        "tpcds_key_t is too small for this scale factor, "
  //        "consider using tpcds_key_t = int64_t;");
  return {k_first_row, k_row_count};
}

std::string boolean_to_string(bool boolean) {
  return {boolean ? "Y" : "N"};
}

std::string zip_to_string(int32_t zip) {
  auto result = std::string(5, '?');
  // NOLINTNEXTLINE(cppcoreguidelines-pro-type-vararg,hicpp-vararg)
  const auto snprintf_rc =
      std::snprintf(result.data(), result.size() + 1, "%05d", zip);
  // Assert(snprintf_rc > 0, "Unexpected string to parse.");
  return result;
}

TPCDSTableGenerator::TPCDSTableGenerator(uint32_t scale_factor,
                                         std::filesystem::path resource_dir,
                                         int rng_seed)
    : _scale_factor{scale_factor} {
  init_tpcds_tools(scale_factor, resource_dir.string(), rng_seed);
  atexit(
      []() { throw std::runtime_error("TPCDSTableGenerator internal error"); });
}

class TableLoader {
 public:
  TableLoader() = default;

  auto& start() {
    sql += "(";
    return *this;
  }

  auto& addItem(auto value) {
    sql += std::format("{},", value);
    return *this;
  }

  auto end() {
    sql += ")";
    return *this;
  }

 private:
  std::string sql;
};

bool TPCDSTableGenerator::generate_call_center(ds_key_t max_rows) const {
  auto [call_center_first, call_center_count] = prepare_for_table(CALL_CENTER);
  call_center_count = std::min(call_center_count, max_rows);

  auto call_center = CALL_CENTER_TBL{};

  TableLoader loader;
  call_center.cc_closed_date_id = ds_key_t{-1};
  for (auto call_center_index = ds_key_t{0};
       call_center_index < call_center_count; ++call_center_index) {
    // mk_w_call_center needs a pointer to the previous result of
    // mk_w_call_center to add "update entries" for the same call center
    mk_w_call_center(&call_center, call_center_first + call_center_index);
    loader.start()
        .addItem(call_center.cc_call_center_sk)
        .addItem(call_center.cc_call_center_id)
        .end();
    //     .add(call_center.cc_rec_start_date)
    //     .add(call_center.cc_rec_end_date)
    //     .add(call_center.cc_closed_date_id);
    tpcds_row_stop(CALL_CENTER);
  }

  return true;
}

}  // namespace tpcds
