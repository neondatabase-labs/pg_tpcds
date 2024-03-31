file(
  GLOB_RECURSE
  TPCDS_SRC
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/dsdgen/*.cpp
)

add_library(tpcds OBJECT
    dsdgen.cpp
    append_info-c.cpp
    dsdgen_helpers.cpp
    ${TPCDS_SRC})

target_include_directories(tpcds PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/include/dedgen
)

target_compile_options(tpcds PUBLIC
    "-fPIC")

set(TPCDS_OBJECT_FILES
    ${TPCDS_OBJECT_FILES} $<TARGET_OBJECTS:tpcds>
    PARENT_SCOPE)
