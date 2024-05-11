
#pragma once

#include <cstdint>
#include <string>

namespace duckdb {
class ClientContext;
}

namespace tpcds {

struct DSDGenWrapper {
  //! Generate the TPC-DS data of the given scale factor
  static void DSDGen(double scale, bool overwrite);

  static uint32_t QueriesCount();
  //! Gets the specified TPC-DS Query number as a string
  static const char *GetQuery(int query);

  static void CreateTPCDSSchema(bool overwrite);

  static void DropTPCDSSchema();
};

}  // namespace tpcds
