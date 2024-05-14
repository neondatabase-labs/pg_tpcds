
#pragma once

#include <cstdint>
#include <string>

namespace duckdb {
class ClientContext;
}

namespace tpcds {

struct tpcds_runner_result {
  bool is_new;
  int qid;
  double duration;
  double checked;
};

struct DSDGenWrapper {
  //! Generate the TPC-DS data of the given scale factor
  // static void DSDGen(double scale, bool overwrite);

  static uint32_t QueriesCount();
  //! Gets the specified TPC-DS Query number as a string
  static const char* GetQuery(int query);

  static void CreateTPCDSSchema();

  static void CleanUpTPCDSSchema();

  static tpcds_runner_result** RunTPCDS(int qid);
};

}  // namespace tpcds
