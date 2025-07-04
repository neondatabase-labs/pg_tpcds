cmake_minimum_required(VERSION 3.20.0)
project(pg_tpcds VERSION
  1.0.0
  LANGUAGES CXX C)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_EXPORT_COMPILE_COMMANDS on)
set(CMAKE_POSITION_INDEPENDENT_CODE TRUE)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_SOURCE_DIR}/cmake)

find_package(PostgreSQL REQUIRED)

add_subdirectory(external/fmt)

# clang-format
add_custom_target(format
  COMMAND git diff --name-only --diff-filter=d --cached '*.cpp' '*.h' | xargs clang-format -i
  WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}
)

file(
  GLOB_RECURSE
  TPCDS_SRCS
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/src/tpcds/*.cpp 
)

add_library(tpcds
  OBJECT
    ${TPCDS_SRCS}
)

target_include_directories(tpcds
  PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/src/tpcds/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src/tpcds/include/dsdgen-c
    ${PostgreSQL_SERVER_INCLUDE_DIRS}
    PUBLIC external/fmt/include
)

target_compile_options(
  tpcds
  PRIVATE
    -Wno-format
    -Wno-unused-result
    -Wno-write-strings
)

add_postgresql_extension(
  pg_tpcds
  VERSION 1.0
  SOURCES src/pg_tpcds.cpp
  SCRIPTS src/pg_tpcds--1.0.sql
  REQUIRES dblink
)

target_link_libraries(pg_tpcds tpcds)
target_compile_definitions(tpcds PRIVATE FMT_HEADER_ONLY=1)

set(TPCDS_CURR_DIR ${CMAKE_CURRENT_SOURCE_DIR}/src/tpcds)
set(TPCDS_INSTALL_DIR ${PostgreSQL_EXTENSION_DIR}/src/tpcds)

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
    ${CMAKE_CURRENT_SOURCE_DIR}/src/pre_prepare.sql
    ${CMAKE_CURRENT_SOURCE_DIR}/src/post_prepare.sql
  DESTINATION
    ${TPCDS_INSTALL_DIR}
)

