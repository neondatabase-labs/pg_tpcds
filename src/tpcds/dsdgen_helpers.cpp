#include "dsdgen_helpers.hpp"

#include <string>

#define DECLARER
#include "dsdgen/address.h"
#include "dsdgen/build_support.h"
#include "dsdgen/config.h"
#include "dsdgen/dist.h"
#include "dsdgen/genrand.h"
#include "dsdgen/init.h"
#include "dsdgen/params.h"
#include "dsdgen/porting.h"
#include "dsdgen/scaling.h"
#include "dsdgen/tdefs.h"

namespace tpcds {

void InitializeDSDgen(double scale) {
  InitConstants::Reset();
  ResetCountCount();
  std::string t = std::to_string(scale);
  set_str("SCALE", (char *)t.c_str());  // set SF, which also does a default
                                        // init (e.g. random seed)
  init_rand();                          // no random numbers without this
}

ds_key_t GetRowCount(int table_id) {
  return get_rowcount(table_id);
}

void ResetCountCount() {
  resetCountCount();
}

tpcds_table_def GetTDefByNumber(int table_id) {
  auto tdef = getSimpleTdefsByNumber(table_id);
  tpcds_table_def def;
  def.name = tdef->name;
  def.fl_child = tdef->flags & FL_CHILD ? 1 : 0;
  def.fl_small = tdef->flags & FL_SMALL ? 1 : 0;
  def.first_column = tdef->nFirstColumn;
  return def;
}

tpcds_builder_func GetTDefFunctionByNumber(int table_id) {
  auto table_funcs = getTdefFunctionsByNumber(table_id);
  return table_funcs->builder;
}

}  // namespace tpcds
