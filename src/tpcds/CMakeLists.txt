set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-format -Wno-implicit-int -Wno-deprecated-declarations")
list(APPEND TPCDS_COMPILE_OPTIONS "-D_FILE_OFFSET_BITS=64" "-D_LARGEFILE_SOURCE" "-DLINUX")
set(TPCDS_INCLUDE_DIRS ${CMAKE_CURRENT_SOURCE_DIR}/include/tpcds)


# mkheader
add_executable(mkheader
  ${CMAKE_CURRENT_SOURCE_DIR}/src/mkheader/mkheader.c)

target_compile_definitions(mkheader
  PRIVATE
    ${TPCDS_COMPILE_OPTIONS}
)

target_include_directories(mkheader PRIVATE ${TPCDS_INCLUDE_DIRS})

add_custom_command(
  OUTPUT
    ${TPCDS_INCLUDE_DIRS}/tables.h
    ${TPCDS_INCLUDE_DIRS}/streams.h
    ${TPCDS_INCLUDE_DIRS}/columns.h
  COMMAND mkheader ${CMAKE_CURRENT_SOURCE_DIR}/src/mkheader/column_list.txt
  DEPENDS mkheader
  WORKING_DIRECTORY ${TPCDS_INCLUDE_DIRS}
)

add_custom_target(
  COLUMS_HEADERS
  DEPENDS
    ${TPCDS_INCLUDE_DIRS}/tables.h
    ${TPCDS_INCLUDE_DIRS}/streams.h
    ${TPCDS_INCLUDE_DIRS}/columns.h
)


# common library dependence on columns headers
file(
  GLOB_RECURSE
  TPCDS_COMMON
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/src/common/*.c
)

add_library(tpcds_common STATIC ${TPCDS_COMMON})

target_include_directories(tpcds_common PUBLIC ${TPCDS_INCLUDE_DIRS})

target_compile_definitions(tpcds_common
  PUBLIC
  ${TPCDS_COMPILE_OPTIONS}
)

target_link_libraries(tpcds_common m)

add_dependencies(tpcds_common COLUMS_HEADERS)


# distcomp
#  gen tpcds.idx.h and tpcds.idx
#  dependence on COLUMS_HEADERS
file(
  GLOB_RECURSE
  DISTCOMP_SRC
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/src/distcomp/*.c
)

add_executable(distcomp ${DISTCOMP_SRC})

target_compile_definitions(distcomp
  PRIVATE
    ${TPCDS_COMPILE_OPTIONS}
)

target_link_libraries(distcomp m)

add_custom_command(
  OUTPUT
    ${TPCDS_INCLUDE_DIRS}/tpcds.idx.h
    ${TPCDS_INCLUDE_DIRS}/tpcds.idx
  COMMAND distcomp -i tpcds.dst -o tpcds.idx
  DEPENDS distcomp
  WORKING_DIRECTORY ${TPCDS_INCLUDE_DIRS}
)

add_custom_target(DISCOMP
  DEPENDS
    ${TPCDS_INCLUDE_DIRS}/tpcds.idx.h
    ${TPCDS_INCLUDE_DIRS}/tpcds.idx
)

target_compile_definitions(distcomp
  PRIVATE
    ${TPCDS_COMPILE_OPTIONS}
)

target_include_directories(distcomp PRIVATE ${TPCDS_INCLUDE_DIRS})

target_link_libraries(distcomp m tpcds_common)

add_dependencies(distcomp COLUMS_HEADERS)

# dsdgen library dependence on COLUMS_HEADERS and DISCOMP
file(
  GLOB_RECURSE
  TPCDS_SRC
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/src/dsdgen/*.c
)

add_library(dsdgen STATIC ${TPCDS_SRC})

target_include_directories(dsdgen PRIVATE ${TPCDS_INCLUDE_DIRS})

target_compile_definitions(dsdgen
  PRIVATE
    ${TPCDS_COMPILE_OPTIONS}
)

target_link_libraries(dsdgen m tpcds_common)

add_dependencies(dsdgen COLUMS_HEADERS DISCOMP)

# dsqgen dependence on COLUMS_HEADERS, DISCOMP
find_package(FLEX REQUIRED)
find_package(BISON 3.5.0 REQUIRED)

list(APPEND BISON_OPTIONS "-d -v")

bison_target(parser
    ${CMAKE_CURRENT_SOURCE_DIR}/src/dsqgen/qgen.y
    ${CMAKE_CURRENT_SOURCE_DIR}/src/dsqgen/y.tab.c
    COMPILE_FLAGS ${BISON_OPTIONS})

flex_target(scanner
    ${CMAKE_CURRENT_SOURCE_DIR}/src/dsqgen/tokenizer.l
    ${CMAKE_CURRENT_SOURCE_DIR}/src/dsqgen/tokenizer.c)

ADD_FLEX_BISON_DEPENDENCY(scanner parser)

file(
  GLOB_RECURSE
  TPCDS_LOAD_SRC
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/src/dsqgen/*.c
)

add_library(dsqgen STATIC
  ${TPCDS_LOAD_SRC}
  ${BISON_parser_OUTPUTS}
  ${FLEX_scanner_OUTPUTS}
)

target_include_directories(dsqgen PRIVATE ${TPCDS_INCLUDE_DIRS})

target_compile_definitions(dsqgen
  PRIVATE
    ${TPCDS_COMPILE_OPTIONS}
)

target_link_libraries(dsqgen m tpcds_common)

add_dependencies(dsqgen DISCOMP)

add_library(tpcds OBJECT
    tpcds_wrapper.cpp)

target_link_libraries(tpcds PUBLIC dsdgen dsqgen tpcds_common)

target_include_directories(tpcds PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${PostgreSQL_SERVER_INCLUDE_DIRS}
)

target_compile_options(tpcds PUBLIC
  "-fPIC")

set(TPCDS_OBJECT_FILES
    ${TPCDS_OBJECT_FILES} $<TARGET_OBJECTS:tpcds>
    PARENT_SCOPE)
