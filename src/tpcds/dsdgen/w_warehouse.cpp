/*
 * Legal Notice
 *
 * This document and associated source code (the "Work") is a part of a
 * benchmark specification maintained by the TPC.
 *
 * The TPC reserves all right, title, and interest to the Work as provided
 * under U.S. and international laws, including without limitation all patent
 * and trademark rights therein.
 *
 * No Warranty
 *
 * 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION
 *     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE
 *     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER
 *     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY,
 *     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES,
 *     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR
 *     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF
 *     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE.
 *     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT,
 *     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT
 *     WITH REGARD TO THE WORK.
 * 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO
 *     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE
 *     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS
 *     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT,
 *     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
 *     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT
 *     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD
 *     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES.
 *
 * Contributors:
 * Gradient Systems
 */
#include "w_warehouse.h"

#include "address.h"

#include "build_support.h"
#include "config.h"
#include "constants.h"
#include "date.h"
#include "decimal.h"
#include "genrand.h"
#include "misc.h"
#include "nulls.h"
#include "porting.h"
#include "tables.h"
#include "tdefs.h"
#include "tpcds_loader.h"

#include <stdio.h>

struct W_WAREHOUSE_TBL g_w_warehouse;

/*
 * mk_warehouse
 */
int mk_w_warehouse(void *info_arr, ds_key_t index) {
  /* begin locals declarations */
  struct W_WAREHOUSE_TBL *r;
  tdef *pT = getSimpleTdefsByNumber(WAREHOUSE);

  r = &g_w_warehouse;

  nullSet(&pT->kNullBitMap, W_NULLS);
  r->w_warehouse_sk = index;
  mk_bkey(&r->w_warehouse_id[0], index, W_WAREHOUSE_ID);
  gen_text(&r->w_warehouse_name[0], W_NAME_MIN, RS_W_WAREHOUSE_NAME, W_WAREHOUSE_NAME);
  r->w_warehouse_sq_ft = genrand_integer(NULL, DIST_UNIFORM, W_SQFT_MIN, W_SQFT_MAX, 0, W_WAREHOUSE_SQ_FT);

  mk_address(&r->w_address, W_WAREHOUSE_ADDRESS);

  char szTemp[128];

  auto &loader = *(tpcds::TableLoader *)info_arr;

  loader.start()
      .addItemKey(r->w_warehouse_sk)
      .addItem(r->w_warehouse_id)
      .addItem(r->w_warehouse_name)
      .addItem(r->w_warehouse_sq_ft)
      .addItem(std::to_string(r->w_address.street_num).c_str())
      .addItemStreet(r->w_address)
      .addItem(r->w_address.street_type)
      .addItem(r->w_address.suite_num)
      .addItem(r->w_address.city)
      .addItem(r->w_address.county)
      .addItem(r->w_address.state)
      .addItem(fmt::format("{:05d}", r->w_address.zip).data())
      .addItem(r->w_address.country)
      .addItem(fmt::format("{}", r->w_address.gmt_offset).data())
      .end();
  return 0;
}
