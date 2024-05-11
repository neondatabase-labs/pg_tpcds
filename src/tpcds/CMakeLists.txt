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
  ${PostgreSQL_SERVER_INCLUDE_DIRS}
)

target_compile_options(tpcds PUBLIC
  "-fPIC")

function(target_embedding_files TARGET_DEPENDS TARGET_FILES TARGET_PATH OUTOUT_PATH)
  file(READ ${TARGET_FILES} content HEX)
  string(REGEX MATCHALL "([A-Fa-f0-9][A-Fa-f0-9])" SEPARATED_HEX ${content})
  file(RELATIVE_PATH FILE_ORINGINAL ${TARGET_PATH} ${TARGET_FILES})
  get_filename_component(FILE ${TARGET_FILES} NAME_WE)

  set(counter 0)
  set(output_hex "")

  foreach (hex IN LISTS SEPARATED_HEX)
    string(APPEND output_hex "0x${hex},")
    MATH(EXPR counter "${counter}+1")
    if(counter GREATER 16)
      string(APPEND output_hex "\n")
      set(counter 0)
    endif()
  endforeach ()

  string(APPEND output_hex "0x00,")

  set(output_hex "
uint8_t __${FILE}[] = {
${output_hex}
}\;
unsigned __${FILE}_size = sizeof(__${FILE})\;
  ")
  
  if (NOT EXISTS ${OUTOUT_PATH})
    file(MAKE_DIRECTORY ${OUTOUT_PATH})
  endif ()

  file(WRITE ${OUTOUT_PATH}/${FILE}.h  ${output_hex})

  add_custom_target(${FILE}Target DEPENDS ${OUTOUT_PATH}/${FILE}.h)
  add_dependencies(${TARGET_DEPENDS} ${FILE}Target)
endfunction(target_embedding_files)

file(
  GLOB_RECURSE
  TPCDS_QUERIES
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/queries/*.sql 
)

foreach(QUERY_CELL ${TPCDS_QUERIES})
  target_embedding_files(tpcds
    ${QUERY_CELL}
    ${CMAKE_CURRENT_SOURCE_DIR}/queries
    ${CMAKE_CURRENT_SOURCE_DIR}/include/queries)
endforeach()

file(
  GLOB_RECURSE
  TPCDS_TABLES
  CONFIGURE_DEPENDS
    ${CMAKE_CURRENT_SOURCE_DIR}/schema/*.sql 
)

foreach(QUERY_CELL ${TPCDS_TABLES})
  target_embedding_files(tpcds
    ${QUERY_CELL}
    ${CMAKE_CURRENT_SOURCE_DIR}/schema
    ${CMAKE_CURRENT_SOURCE_DIR}/include/schema)
endforeach()

set(TPCDS_OBJECT_FILES
    ${TPCDS_OBJECT_FILES} $<TARGET_OBJECTS:tpcds>
    PARENT_SCOPE)
