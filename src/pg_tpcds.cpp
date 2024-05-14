
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

static bool tpcds_prepare() {
  try {
    tpcds::DSDGenWrapper::CreateTPCDSSchema();
  } catch (const std::exception& e) {
    elog(ERROR, "TPC-DS Failed to prepare schema, get error: %s", e.what());
  }
  return true;
}

static bool tpcds_cleanup() {
  try {
    tpcds::DSDGenWrapper::CleanUpTPCDSSchema();
  } catch (const std::exception& e) {
    elog(ERROR, "TPC-DS Failed to cleanup, get error: %s", e.what());
  }
  return true;
}

static const char* tpcds_queries(int qid) {
  try {
    return tpcds::DSDGenWrapper::GetQuery(qid);
  } catch (const std::exception& e) {
    elog(ERROR, "TPC-DS Failed to get query, get error: %s", e.what());
  }
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

static tpcds_runner_result** tpcds_runner(int qid) {
  try {
    return tpcds::DSDGenWrapper::RunTPCDS(qid);
  } catch (const std::exception& e) {
    elog(ERROR, "TPC-DS Failed to run query, get error: %s", e.what());
  }
}

}  // namespace tpcds

extern "C" {

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(tpcds_prepare);
Datum tpcds_prepare(PG_FUNCTION_ARGS) {
  bool result = tpcds::tpcds_prepare();

  PG_RETURN_BOOL(result);
}

PG_FUNCTION_INFO_V1(tpcds_cleanup);
Datum tpcds_cleanup(PG_FUNCTION_ARGS) {
  bool result = tpcds::tpcds_cleanup();

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

PG_FUNCTION_INFO_V1(tpcds_runner);

Datum tpcds_runner(PG_FUNCTION_ARGS) {
  int qid = PG_GETARG_INT32(0);

  ReturnSetInfo* rsinfo = (ReturnSetInfo*)fcinfo->resultinfo;

  Datum values[3];
  bool nulls[3] = {false, false, false};

  tpcds::tpcds_runner_result** result = tpcds::tpcds_runner(qid);

  InitMaterializedSRF(fcinfo, 0);

  if (qid == 0) {
    for (int i = 0; i < tpcds::tpcds_num_queries(); i++) {
      values[0] = result[i]->qid;
      values[1] = Float8GetDatum(result[i]->duration);
      values[2] = BoolGetDatum(result[i]->checked);
      tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
    }
  } else {
    values[0] = result[0]->qid;
    values[1] = Float8GetDatum(result[0]->duration);
    values[2] = BoolGetDatum(result[0]->checked);
    tuplestore_putvalues(rsinfo->setResult, rsinfo->setDesc, values, nulls);
  }

  return 0;
}
}