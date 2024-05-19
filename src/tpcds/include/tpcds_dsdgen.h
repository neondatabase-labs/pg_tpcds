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
  TPCDSTableGenerator(uint32_t scale_factor, std::filesystem::path resource_dir,
                      int rng_seed = 19620718);

  bool generate_call_center(ds_key_t max_rows = _ds_key_max) const;

 private:
  static constexpr auto _ds_key_max = std::numeric_limits<ds_key_t>::max();
  uint32_t _scale_factor;
};

}  // namespace tpcds
