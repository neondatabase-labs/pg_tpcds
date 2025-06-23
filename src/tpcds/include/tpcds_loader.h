#pragma once

#include <format>
#include <stdexcept>
#include "date.h"

extern "C" {
#include <postgres.h>

#include <access/table.h>
#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <utils/builtins.h>
#include <utils/lsyscache.h>

// TODO split pg functions into other file

#ifdef snprintf
#undef snprintf
#endif
}

#include "tpcds_dsdgen.h"

#include "address.h"
#include "date.h"
#include "decimal.h"
#include "nulls.h"
#include "porting.h"
#include "r_params.h"

namespace tpcds {

class TableLoader {
 public:
  TableLoader(const tpcds_table_def* table_def) : table_def(table_def) {
    reloid_ = DirectFunctionCall1(regclassin, CStringGetDatum(table_def->name));
    rel_ = try_table_open(reloid_, NoLock);
    if (!rel_)
      throw std::runtime_error("try_table_open Failed");

    auto tupDesc = RelationGetDescr(rel_);
    Oid in_func_oid;

    in_functions = new FmgrInfo[tupDesc->natts];
    typioparams = new Oid[tupDesc->natts];

    for (auto attnum = 1; attnum <= tupDesc->natts; attnum++) {
      Form_pg_attribute att = TupleDescAttr(tupDesc, attnum - 1);

      getTypeInputInfo(att->atttypid, &in_func_oid, &typioparams[attnum - 1]);
      fmgr_info(in_func_oid, &in_functions[attnum - 1]);
    }

    slot = MakeSingleTupleTableSlot(tupDesc, &TTSOpsMinimalTuple);
    slot->tts_tableOid = RelationGetRelid(rel_);
  };

  ~TableLoader() {
    table_close(rel_, NoLock);
    free(in_functions);
    free(typioparams);
    ExecDropSingleTupleTableSlot(slot);
  }

  bool ColnullCheck() { return nullCheck(table_def->first_column + current_item_); }

  auto& nullItem() {
    slot->tts_isnull[current_item_] = true;
    current_item_++;
    return *this;
  }

  template <typename T>
  auto& addItemInternal(T value) {
    Datum datum;
    if constexpr (std::is_same_v<T, char*> || std::is_same_v<T, const char*> || std::is_same_v<T, char>) {
      if (value == nullptr)
        slot->tts_isnull[current_item_] = true;
      else
        slot->tts_values[current_item_] = DirectFunctionCall3(
            in_functions[current_item_].fn_addr, CStringGetDatum(value), ObjectIdGetDatum(typioparams[current_item_]),
            TupleDescAttr(RelationGetDescr(rel_), current_item_)->atttypmod);
    } else
      slot->tts_values[current_item_] = value;

    current_item_++;
    return *this;
  }

  template <typename T>
  auto& addItem(T value) {
    if (ColnullCheck()) {
      return nullItem();
    } else {
      return addItemInternal(value);
    }
  }

  auto& addItemBool(bool value) {
    if (ColnullCheck()) {
      return nullItem();
    } else {
      return addItemInternal(value ? "Y" : "N");
    }
  }

  auto& addItemKey(ds_key_t value) {
    if (ColnullCheck() || value == -1) {
      return nullItem();
    } else {
      return addItemInternal(value);
    }
  }

  auto& addItemDecimal(decimal_t& decimal) {
    if (ColnullCheck()) {
      return nullItem();
    } else {
      // from print
      double dTemp;

      dTemp = decimal.number;
      for (auto i = 0; i < decimal.precision; i++)
        dTemp /= 10.0;

      char fpOutfile[15] = {0};
      sprintf(fpOutfile, "%.*f", decimal.precision, dTemp);

      return addItemInternal(fpOutfile);
    }
  }

  auto& addItemStreet(const ds_addr_t& address) {
    if (ColnullCheck()) {
      return nullItem();
    } else {
      if (address.street_name2 == nullptr) {
        return addItemInternal(address.street_name1);
      } else {
        auto s = std::string{address.street_name1} + " " + address.street_name2;
        return addItemInternal(s.c_str());
      }
    }
  }

  auto& addItemDate(ds_key_t value) {
    if (ColnullCheck() || value <= 0) {
      return nullItem();
    } else {
      auto date = date_t{};
      jtodt(&date, static_cast<int>(value));

      auto s = std::format("{:4d}-{:02d}-{:02d}", date.year, date.month, date.day);
      return addItemInternal(s.data());
    }
  }

  auto& start() {
    ExecClearTuple(slot);
    MemSet(slot->tts_values, 0, RelationGetDescr(rel_)->natts * sizeof(Datum));
    MemSet(slot->tts_isnull, false, RelationGetDescr(rel_)->natts * sizeof(bool));
    current_item_ = 0;
    return *this;
  }

  auto& end() {
    ExecStoreVirtualTuple(slot);

    table_tuple_insert(rel_, slot, mycid, ti_options, NULL);

    row_count_++;
    return *this;
  }

  auto row_count() const { return row_count_; }

  Oid reloid_;
  Relation rel_;
  size_t row_count_ = 0;
  size_t current_item_ = 0;

  FmgrInfo* in_functions;
  Oid* typioparams;
  TupleTableSlot* slot;
  CommandId mycid = GetCurrentCommandId(true);
  int ti_options = (TABLE_INSERT_SKIP_FSM | TABLE_INSERT_FROZEN | TABLE_INSERT_NO_LOGICAL);

  const tpcds_table_def* table_def;
};

}  // namespace tpcds
