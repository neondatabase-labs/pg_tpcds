#pragma once

#include <string>
#include <vector>

class TPCDSMain {
 public:
  static bool tpcds_prepare(bool overwrite);
  static bool tpcds_destroy();
  static std::vector<std::string> tpcds_queries();
};