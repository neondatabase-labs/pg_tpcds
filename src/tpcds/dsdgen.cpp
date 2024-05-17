#define ENABLE_NLS

extern "C" {
#include <postgres.h>

#include <executor/spi.h>
#include <lib/stringinfo.h>
#include <libpq/pqformat.h>
#include <miscadmin.h>
#include <utils/builtins.h>

#include <string.h>
}
#include <algorithm>
#include <cassert>
#include <exception>
#include <filesystem>
#include <format>
#include <fstream>
#include <ranges>
#include <stdexcept>

#include "append_info-c.hpp"
#include "dsdgen.hpp"
#include "dsdgen/tdefs.h"
#include "dsdgen_helpers.hpp"
#include "tpcds_constants.hpp"

namespace tpcds {

static auto get_extension_external_directory(void) {
  char sharepath[MAXPGPATH];

  get_share_path(my_exec_path, sharepath);
  auto path = std::format("{}/extension/tpcds", sharepath);
  return std::move(path);
}

class Executor {
 public:
  Executor(const Executor &other) = delete;
  Executor &operator=(const Executor &other) = delete;

  Executor() {
    if (SPI_connect() != SPI_OK_CONNECT)
      throw std::runtime_error("SPI_connect Failed");
  }

  ~Executor() { SPI_finish(); }

  void execute(const std::string &query) const {
    if (auto ret = SPI_exec(query.c_str(), 0); ret < 0)
      throw std::runtime_error(std::format("SPI_exec Failed, get {}", ret));
  }
};

[[maybe_unused]] static double exec_spec(const auto &path,
                                         const Executor &executor) {
  if (std::filesystem::exists(path)) {
    std::ifstream file(path);
    std::string sql((std::istreambuf_iterator<char>(file)),
                    std::istreambuf_iterator<char>());
    const auto start = std::chrono::high_resolution_clock::now();
    executor.execute(sql);
    const auto end = std::chrono::high_resolution_clock::now();
    return std::chrono::duration<double, std::milli>(end - start).count();
  }
  return 0;
}

void DSDGenWrapper::CreateTPCDSSchema() {
  const std::filesystem::path extension_dir =
      get_extension_external_directory();

  Executor executor;

  exec_spec(extension_dir / "pre_prepare.sql", executor);

  auto schema = extension_dir / "schema";
  if (std::filesystem::exists(schema)) {
    std::ranges::for_each(
        std::filesystem::directory_iterator(schema), [&](const auto &entry) {
          std::ifstream file(entry.path());
          std::string sql((std::istreambuf_iterator<char>(file)),
                          std::istreambuf_iterator<char>());
          executor.execute(sql);
        });
  } else
    throw std::runtime_error("Schema file does not exist");

  exec_spec(extension_dir / "post_prepare.sql", executor);
}

void DSDGenWrapper::CleanUpTPCDSSchema() {
  Executor executor;
  const std::filesystem::path extension_dir =
      get_extension_external_directory();
  exec_spec(extension_dir / "pg_tpcds_cleanup.sql", executor);
}

uint32_t DSDGenWrapper::QueriesCount() {
  return TPCDS_QUERIES_COUNT;
}

const char *DSDGenWrapper::GetQuery(int query) {
  if (query <= 0 || query > TPCDS_QUERIES_COUNT) {
    throw std::runtime_error(
        std::format("Out of range TPC-DS query number {}", query));
  }

  const std::filesystem::path extension_dir =
      get_extension_external_directory();

  auto queries = extension_dir / "queries" / std::format("{:02d}.sql", query);
  if (std::filesystem::exists(queries)) {
    std::ifstream file(queries);
    std::string sql((std::istreambuf_iterator<char>(file)),
                    std::istreambuf_iterator<char>());

    return strdup(sql.c_str());
  }
  throw std::runtime_error("Queries file does not exist");
}

tpcds_runner_result *DSDGenWrapper::RunTPCDS(int qid) {
  if (qid < 0 || qid > TPCDS_QUERIES_COUNT) {
    throw std::runtime_error(
        std::format("Out of range TPC-DS query number {}", qid));
  }

  const std::filesystem::path extension_dir =
      get_extension_external_directory();

  Executor executor;

  auto queries = extension_dir / "queries" / std::format("{:02d}.sql", qid);

  if (std::filesystem::exists(queries)) {
    auto *result = (tpcds_runner_result *)palloc(sizeof(tpcds_runner_result));

    result->qid = qid;
    result->duration = exec_spec(queries, executor);

    // TODO: check result
    result->checked = true;

    return result;
  } else
    throw std::runtime_error(
        std::format("Queries file for qid: {} does not exist", qid));
}
}  // namespace tpcds
