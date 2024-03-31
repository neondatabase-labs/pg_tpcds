
add_subdirectory(tpcds)

add_postgresql_extension(
  pg_tpcds
  VERSION 1.0
  SOURCES pg_tpcds.cpp
  SCRIPTS pg_tpcds--1.0.sql
  DEPENDS_LIB ${TPCDS_OBJECT_FILES})
