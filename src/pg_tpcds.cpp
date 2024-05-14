
extern "C" {
#include <postgres.h>
#include <fmgr.h>

#include <access/htup_details.h>
#include <executor/spi.h>
#include <funcapi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <utils/builtins.h>

#include <string.h>
}

#include "tpcds/include/dsdgen.hpp"

namespace tpcds {

static bool tpcds_prepare(bool overwrite) {
  try {
    tpcds::DSDGenWrapper::CreateTPCDSSchema(overwrite);
  } catch (const std::exception& e) {
    // error
    return false;
  }
  return true;
}

static bool tpcds_destroy() {
  try {
    tpcds::DSDGenWrapper::DropTPCDSSchema();
  } catch (const std::exception& e) {
    return false;
  }
  return true;
}

static const char* tpcds_queries(int qid) {
  return tpcds::DSDGenWrapper::GetQuery(qid);
}

static int tpcds_num_queries() {
  return tpcds::DSDGenWrapper::QueriesCount();
}

// static bool dsdgen(double scale_factor, bool overwrite) {
//   try {
//     tpcds::DSDGenWrapper::DSDGen(scale_factor, overwrite);
//   } catch (const std::exception& e) {
//     return false;
//   }
//   return true;
// }

}  // namespace tpcds

extern "C" {

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(tpcds_prepare);
Datum tpcds_prepare(PG_FUNCTION_ARGS) {
  bool overwrite = PG_GETARG_BOOL(0);

  bool result = tpcds::tpcds_prepare(overwrite);

  PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(tpcds_destroy);
Datum tpcds_destroy(PG_FUNCTION_ARGS) {
  bool result = tpcds::tpcds_destroy();

  PG_RETURN_BOOL(result);
}

/*
 * tpcds_queries
 */
PG_FUNCTION_INFO_V1(tpcds_queries);

Datum tpcds_queries(PG_FUNCTION_ARGS) {
  ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;
  Datum values[2];
  bool nulls[2] = {false, false};

  int get_qid = PG_GETARG_INT32(0);
  InitMaterializedSRF(fcinfo, 0);

  if (get_qid == 0) {
    int q_count = tpcds::tpcds_num_queries();
    int qid = 0;
    while (qid < q_count) {
      const char* query = tpcds::tpcds_queries(++qid);

      values[0] = qid;
      values[1] = CStringGetTextDatum(query);

      tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    }
  } else {
    const char* query = tpcds::tpcds_queries(get_qid);
    values[0] = get_qid;
    values[1] = CStringGetTextDatum(query);
    tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
  }
  return 0;
}

// PG_FUNCTION_INFO_V1(dsdgen);

// Datum dsdgen(PG_FUNCTION_ARGS) {
//   double sf = PG_GETARG_FLOAT8(0);
//   bool overwrite = PG_GETARG_BOOL(1);

//   bool result = tpcds::dsdgen(sf, overwrite);

//   PG_RETURN_BOOL(result);
// }
}