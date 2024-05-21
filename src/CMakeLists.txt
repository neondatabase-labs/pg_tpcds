
add_subdirectory(tpcds)

message("TPCDS_OBJECT_FILES: ${TPCDS_OBJECT_FILES}")
add_postgresql_extension(
  pg_tpcds
  VERSION 1.0
  SOURCES pg_tpcds.cpp
  SCRIPTS pg_tpcds--1.0.sql)

target_link_libraries(pg_tpcds tpcds)
# pre_prepare.sql
# post_prepare.sql

set(TPCDS_CURR_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tpcds)
set(TPCDS_INSTALL_DIR ${PostgreSQL_EXTENSION_DIR}/tpcds)

install(
  DIRECTORY
    ${TPCDS_CURR_DIR}/schema
  DESTINATION
    ${TPCDS_INSTALL_DIR}
)

install(
  DIRECTORY
    ${TPCDS_CURR_DIR}/queries
  DESTINATION
    ${TPCDS_INSTALL_DIR}
)

install(
  FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/pre_prepare.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/post_prepare.sql
  DESTINATION
    ${TPCDS_INSTALL_DIR}
)

install(
  FILES
    ${CMAKE_CURRENT_SOURCE_DIR}/tpcds/tpcds-kit/tools/tpcds.idx
  DESTINATION
    ${TPCDS_INSTALL_DIR}
)