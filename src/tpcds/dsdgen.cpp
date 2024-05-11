#define ENABLE_NLS

extern "C" {
#include <postgres.h>

#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <utils/builtins.h>

#include <string.h>
}
#include <algorithm>
#include <cassert>
#include <exception>
#include <filesystem>
#include <format>
#include <fstream>
#include <ranges>
#include <stdexcept>

#include "append_info-c.hpp"
#include "dsdgen.hpp"
#include "dsdgen/tdefs.h"
#include "dsdgen_helpers.hpp"
#include "tpcds_constants.hpp"

namespace tpcds {

void DSDGenWrapper::CreateTPCDSSchema(bool overwrite) {
  if (SPI_connect() != SPI_OK_CONNECT)
    throw std::runtime_error(std::format("SPI_connect Failed"));

  std::ranges::for_each(TPCDS_TABLES, [&](const auto &table) {
    if (SPI_exec(table, 0) != SPI_OK_UTILITY)
      throw std::runtime_error(std::format("Failed to create table {}", table));
  });

  if (SPI_finish() != SPI_OK_FINISH)
    throw std::runtime_error(std::format("SPI_finish Failed"));
}

void DSDGenWrapper::DropTPCDSSchema() {
  if (SPI_connect() != SPI_OK_CONNECT)
    throw std::runtime_error(std::format("SPI_connect Failed"));

  std::ranges::for_each(std::views::iota(0, 24), [&](auto i) {
    auto sql = std::format("DROP TABLE IF EXISTS {};", getTableNameByID(i));
    if (SPI_exec(sql.c_str(), 0) != SPI_OK_UTILITY)
      throw std::runtime_error(
          std::format("Failed to drop table {}", getTableNameByID(i)));
  });

  if (SPI_finish() != SPI_OK_FINISH)
    throw std::runtime_error(std::format("SPI_finish Failed"));
}

void DSDGenWrapper::DSDGen(double scale, bool overwrite) {
  if (scale <= 0) {
    // schema only
    return;
  }

  InitializeDSDgen(scale);

  // populate append info

  int tmin = CALL_CENTER, tmax = DBGEN_VERSION;

  for (int table_id = tmin; table_id < tmax; table_id++) {
    auto table_def = GetTDefByNumber(table_id);
  }

  // actually generate tables using modified data generator functions
  for (int table_id = tmin; table_id < tmax; table_id++) {
    // child tables are created in parent loaders
    if (append_info[table_id]->table_def.fl_child) {
      continue;
    }

    ds_key_t k_row_count = GetRowCount(table_id), k_first_row = 1;

    // TODO: verify this is correct and required here
    if (append_info[table_id]->table_def.fl_small) {
      ResetCountCount();
    }

    auto builder_func = GetTDefFunctionByNumber(table_id);
    assert(builder_func);

    for (ds_key_t i = k_first_row; k_row_count; i++, k_row_count--) {
      if (k_row_count % 1000 == 0 && context.interrupted) {
        throw std::runtime_error("Query execution interrupted");
      }
      // append happens directly in builders since they dump child tables
      // immediately
      if (builder_func((void *)&append_info, i)) {
        throw std::runtime_error("Table generation failed");
      }
    }
  }

  // flush any incomplete chunks
  for (int table_id = tmin; table_id < tmax; table_id++) {
    append_info[table_id]->appender.Close();
  }
}

uint32_t DSDGenWrapper::QueriesCount() {
  return TPCDS_QUERIES_COUNT;
}

const char *DSDGenWrapper::GetQuery(int query) {
  if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
    throw std::runtime_error(
        std::format("Out of range TPC-DS query number {}", query));
  }
  return TPCDS_QUERIES[query - 1];
}

}  // namespace tpcds
