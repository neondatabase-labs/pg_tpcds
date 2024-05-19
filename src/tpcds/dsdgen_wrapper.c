
#define DECLARER
#include <stdio.h>
#include <stdlib.h>
#include <time.h>

#include "config.h"
#include "porting.h"
#ifdef WIN32
#include <direct.h>
#include <process.h>
#endif
#ifdef USE_STRING_H
#include <string.h>
#else
#include <strings.h>
#endif
#include "address.h" /* for access to resetCountyCount() */
#include "build_support.h"
#include "config.h"
#include "date.h"
#include "decimal.h"
#include "dsdgen_wrapper.h"
#include "error_msg.h"
#include "genrand.h"
#include "grammar_support.h" /* to get definition of file_ref_t */
#include "load.h"
#include "parallel.h"
#include "params.h"
#include "print.h"
#include "release.h"
#include "scaling.h"
#include "scd.h"
#include "tables.h"
#include "tdef_functions.h"
#include "tdefs.h"
#include "tpcds.idx.h"

extern int optind, opterr;
extern char *optarg;
file_ref_t CurrentFile;
file_ref_t *pCurrentFile;

/*
 * Routine: find_table(char *, char *)
 * Purpose: partial match routine for arguments to -T
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int find_table(char *szParamName, char *tname) {
  int i, res = -1, nNotSure = -1;
  tdef *pT;
  tdef *pN;

  if (!strcmp(tname, "ALL"))
    return (0);

  for (i = 0; i <= MAX_TABLE; i++) {
    pT = getSimpleTdefsByNumber(i);
    if (strcasecmp(szParamName, "ABREVIATION")) {
      /* if we match the name exactly, then return the result */
      if (!strcasecmp(tname, pT->name))
        return (i);

      /* otherwise, look for sub-string matches */
      if (!strncasecmp(tname, pT->name, strlen(tname))) {
        if (res == -1)
          res = i;
        else
          nNotSure = i;
      }
    } else {
      if (!strcasecmp(tname, pT->abreviation))
        return (i);
    }
  }

  if (res == -1) {
    fprintf(stdout, "ERROR: No match found for table name '%s'\n", tname);
    exit(1);
  }

  pT = getSimpleTdefsByNumber(res);
  if ((nNotSure != -1) && !(pT->flags & FL_DUP_NAME)) {
    pN = getSimpleTdefsByNumber(nNotSure);
    fprintf(stdout,
            "WARNING: Table name '%s' not unique. Could be '%s' or '%s'. "
            "Assuming '%s'\n",
            tname, pT->name, pN->name, pT->name);
    pT->flags |= FL_DUP_NAME;
  }

  return (res);
}
/*
 * re-set default output file names
 */
/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: None
 */
int set_files(int i) {
  char line[80], *new_name;
  tdef *pT = getSimpleTdefsByNumber(i);

  printf("Enter new destination for %s data: ", pT->name);
  if (fgets(line, sizeof(line), stdin) == NULL)
    return (-1);
  if ((new_name = strchr(line, '\n')) != NULL)
    *new_name = '\0';
  if (strlen(line) == 0)
    return (0);
  new_name = (char *)malloc(strlen(line) + 1);
  MALLOC_CHECK(new_name);
  strcpy(new_name, line);
  pT->name = new_name;

  return (0);
}

/*
 * generate a particular table
 */
/*
 * Routine:
 * Purpose:
 * Algorithm:
 * Data Structures:
 *
 * Params:
 * Returns:
 * Called By:
 * Calls:
 * Assumptions:
 * Side Effects:
 * TODO: 20011217 JMS need to build date-correlated (i.e. fact) tables in proper
 * order
 */
void gen_tbl(int tabid, ds_key_t kFirstRow, ds_key_t kRowCount) {
  int direct, bIsVerbose, nLifeFreq, nMultiplier, nChild;
  ds_key_t i, kTotalRows;
  tdef *pT = getSimpleTdefsByNumber(tabid);
  tdef *pC;
  table_func_t *pF = getTdefFunctionsByNumber(tabid);

  kTotalRows = kRowCount;
  direct = is_set("DBLOAD");
  bIsVerbose = is_set("VERBOSE") && !is_set("QUIET");
  /**
  set the frequency of progress updates for verbose output
  to greater of 1000 and the scale base
  */
  nLifeFreq = 1;
  nMultiplier = dist_member(NULL, "rowcounts", tabid + 1, 2);
  for (i = 0; nLifeFreq < nMultiplier; i++)
    nLifeFreq *= 10;
  if (nLifeFreq < 1000)
    nLifeFreq = 1000;

  if (bIsVerbose) {
    if (pT->flags & FL_PARENT) {
      nChild = pT->nParam;
      pC = getSimpleTdefsByNumber(nChild);
      fprintf(stderr, "%s %s and %s ... ", (direct) ? "Loading" : "Writing",
              pT->name, pC->name);
    } else
      fprintf(stderr, "%s %s ... ", (direct) ? "Loading" : "Writing", pT->name);
  }

  /*
   * small tables use a constrained set of geography information
   */
  if (pT->flags & FL_SMALL)
    resetCountCount();

  for (i = kFirstRow; kRowCount; i++, kRowCount--) {
    if (bIsVerbose && i && (i % nLifeFreq) == 0)
      fprintf(stderr, "%3d%%\b\b\b\b",
              (int)(((kTotalRows - kRowCount) * 100) / kTotalRows));

    /* not all rows that are built should be printed. Use return code to
     * deterine output */
    if (!pF->builder(NULL, i))
      if (pF->loader[direct](NULL)) {
        fprintf(stderr, "ERROR: Load failed on %s!\n", getTableNameByID(tabid));
        exit(-1);
      }
    row_stop(tabid);
  }
  if (bIsVerbose)
    fprintf(stderr, "Done    \n");
  print_close(tabid);

  return;
}

char *dsdgen_internal(char *scale, char *table, char *dir, int overwrite) {
  int i, tabid = -1, nArgLength;
  char *tname;
  ds_key_t kRowCount, kFirstRow;
  struct timeb t;
  tdef *pT;
  table_func_t *pF;

  {
    init_params();
    set_option("SCALE", scale);
    set_option("TABLE", table);
    set_option("DIR", dir);
    // set_option("OVERWRITE", "true");
  }

  init_rand();

  /***
   * traverse the tables, invoking the appropriate data generation routine
   * for any to be built; skip any non-op tables or any child tables (their
   * generation routines are called when the parent is built
   */
  tname = get_str("TABLE");
  if (strcmp(tname, "ALL")) {
    tabid = find_table("TABLE", tname);
  }

  for (i = CALL_CENTER; (pT = getSimpleTdefsByNumber(i)); i++) {
    if (!pT->name)
      break;

    pF = getTdefFunctionsByNumber(i);

    if (pT->flags & FL_NOP) {
      if (tabid == i)
        ReportErrorNoLine(QERR_TABLE_NOP, pT->name, 1);
      continue; /* skip any tables that are not implemented */
    }
    if (pT->flags & FL_CHILD) {
      if (tabid == i)
        ReportErrorNoLine(QERR_TABLE_CHILD, pT->name, 1);
      continue; /* children are generated by the parent call */
    }
    if ((tabid != -1) && (i != tabid))
      continue; /* only generate a table that is explicitly named */

    /**
     * GENERAL CASE
     * if there are no rows to build, then loop
     */
    split_work(i, &kFirstRow, &kRowCount);
    /*
     * if there are rows to skip then skip them
     */
    if (kFirstRow != 1) {
      row_skip(i, (int)(kFirstRow - 1));
      if (pT->flags & FL_PARENT)
        row_skip(pT->nParam, (int)(kFirstRow - 1));
    }

    /*
     * now build the actual rows
     */
    gen_tbl(i, kFirstRow, kRowCount);
  }

  return (0);
}
