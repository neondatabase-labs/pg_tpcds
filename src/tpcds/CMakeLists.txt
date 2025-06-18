# Build tpcds_dbgen library
set(TPCDS_KIT_DIR ${CMAKE_CURRENT_SOURCE_DIR}/tpcds-kit/tools)
add_library(tpcds_dbgen
  STATIC
  ${TPCDS_KIT_DIR}/address.c
  ${TPCDS_KIT_DIR}/build_support.c
  ${TPCDS_KIT_DIR}/date.c
  ${TPCDS_KIT_DIR}/decimal.c
  ${TPCDS_KIT_DIR}/dist.c
  ${TPCDS_KIT_DIR}/modified_driver.c
  ${TPCDS_KIT_DIR}/error_msg.c
  ${TPCDS_KIT_DIR}/genrand.c
  ${TPCDS_KIT_DIR}/join.c
  ${TPCDS_KIT_DIR}/list.c
  ${TPCDS_KIT_DIR}/load.c
  ${TPCDS_KIT_DIR}/misc.c
  ${TPCDS_KIT_DIR}/nulls.c
  ${TPCDS_KIT_DIR}/parallel.c
  ${TPCDS_KIT_DIR}/permute.c
  ${TPCDS_KIT_DIR}/pricing.c
  ${TPCDS_KIT_DIR}/print.c
  ${TPCDS_KIT_DIR}/r_params.c
  ${TPCDS_KIT_DIR}/StringBuffer.c
  ${TPCDS_KIT_DIR}/tdef_functions.c
  ${TPCDS_KIT_DIR}/tdefs.c
  ${TPCDS_KIT_DIR}/text.c
  ${TPCDS_KIT_DIR}/scd.c
  ${TPCDS_KIT_DIR}/scaling.c
  ${TPCDS_KIT_DIR}/release.c
  ${TPCDS_KIT_DIR}/sparse.c
  ${TPCDS_KIT_DIR}/validate.c

  ${TPCDS_KIT_DIR}/dbgen_version.c

  ${TPCDS_KIT_DIR}/s_brand.c
  ${TPCDS_KIT_DIR}/s_call_center.c
  ${TPCDS_KIT_DIR}/s_catalog.c
  ${TPCDS_KIT_DIR}/s_catalog_order.c
  ${TPCDS_KIT_DIR}/s_catalog_order_lineitem.c
  ${TPCDS_KIT_DIR}/s_catalog_page.c
  ${TPCDS_KIT_DIR}/s_catalog_promotional_item.c
  ${TPCDS_KIT_DIR}/s_catalog_returns.c
  ${TPCDS_KIT_DIR}/s_category.c
  ${TPCDS_KIT_DIR}/s_class.c
  ${TPCDS_KIT_DIR}/s_company.c
  ${TPCDS_KIT_DIR}/s_customer_address.c
  ${TPCDS_KIT_DIR}/s_customer.c
  ${TPCDS_KIT_DIR}/s_division.c
  ${TPCDS_KIT_DIR}/s_inventory.c
  ${TPCDS_KIT_DIR}/s_item.c
  ${TPCDS_KIT_DIR}/s_manager.c
  ${TPCDS_KIT_DIR}/s_manufacturer.c
  ${TPCDS_KIT_DIR}/s_market.c
  ${TPCDS_KIT_DIR}/s_pline.c
  ${TPCDS_KIT_DIR}/s_product.c
  ${TPCDS_KIT_DIR}/s_promotion.c
  ${TPCDS_KIT_DIR}/s_purchase.c
  ${TPCDS_KIT_DIR}/s_reason.c
  ${TPCDS_KIT_DIR}/s_store.c
  ${TPCDS_KIT_DIR}/s_store_promotional_item.c
  ${TPCDS_KIT_DIR}/s_store_returns.c
  ${TPCDS_KIT_DIR}/s_subcategory.c
  ${TPCDS_KIT_DIR}/s_subclass.c
  ${TPCDS_KIT_DIR}/s_warehouse.c
  ${TPCDS_KIT_DIR}/s_web_order.c
  ${TPCDS_KIT_DIR}/s_web_order_lineitem.c
  ${TPCDS_KIT_DIR}/s_web_page.c
  ${TPCDS_KIT_DIR}/s_web_promotinal_item.c
  ${TPCDS_KIT_DIR}/s_web_returns.c
  ${TPCDS_KIT_DIR}/s_web_site.c
  ${TPCDS_KIT_DIR}/s_zip_to_gmt.c

  ${TPCDS_KIT_DIR}/w_call_center.c
  ${TPCDS_KIT_DIR}/w_catalog_page.c
  ${TPCDS_KIT_DIR}/w_catalog_returns.c
  ${TPCDS_KIT_DIR}/w_catalog_sales.c
  ${TPCDS_KIT_DIR}/w_customer_address.c
  ${TPCDS_KIT_DIR}/w_customer.c
  ${TPCDS_KIT_DIR}/w_customer_demographics.c
  ${TPCDS_KIT_DIR}/w_datetbl.c
  ${TPCDS_KIT_DIR}/w_household_demographics.c
  ${TPCDS_KIT_DIR}/w_income_band.c
  ${TPCDS_KIT_DIR}/w_inventory.c
  ${TPCDS_KIT_DIR}/w_item.c
  ${TPCDS_KIT_DIR}/w_promotion.c
  ${TPCDS_KIT_DIR}/w_reason.c
  ${TPCDS_KIT_DIR}/w_ship_mode.c
  ${TPCDS_KIT_DIR}/w_store.c
  ${TPCDS_KIT_DIR}/w_store_returns.c
  ${TPCDS_KIT_DIR}/w_store_sales.c
  ${TPCDS_KIT_DIR}/w_timetbl.c
  ${TPCDS_KIT_DIR}/w_warehouse.c
  ${TPCDS_KIT_DIR}/w_web_page.c
  ${TPCDS_KIT_DIR}/w_web_returns.c
  ${TPCDS_KIT_DIR}/w_web_sales.c
  ${TPCDS_KIT_DIR}/w_web_site.c)

target_include_directories(tpcds_dbgen
  PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR})

target_compile_definitions(
  tpcds_dbgen
  PUBLIC
  -DLINUX)

target_compile_options(
  tpcds_dbgen
  PRIVATE

  -Wno-format
  -Wno-implicit-int
  -Wno-incompatible-pointer-types
  -Wno-unused-result
)

target_compile_options(tpcds_dbgen PUBLIC
  "-fPIC")

if ("${CMAKE_CXX_COMPILER_ID}" STREQUAL "Clang")
target_compile_options(
  tpcds_dbgen
  PRIVATE

  -Wno-parentheses-equality
)
endif()

add_library(tpcds STATIC
tpcds_dsdgen.cpp tpcds_wrapper.cpp)

find_package(fmt REQUIRED)

target_link_libraries(tpcds tpcds_dbgen fmt::fmt)

target_include_directories(tpcds PUBLIC
  ${CMAKE_CURRENT_SOURCE_DIR}/include
  ${PostgreSQL_SERVER_INCLUDE_DIRS}
)

target_compile_options(tpcds PUBLIC
  "-fPIC")

# set(TPCDS_OBJECT_FILES
#     ${TPCDS_OBJECT_FILES} $<TARGET_OBJECTS:tpcds>
#     PARENT_SCOPE)
