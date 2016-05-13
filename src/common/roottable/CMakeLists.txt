
SET(ROOTTABLE_SOURCES
	ob_old_root_table_schema.cc
	ob_tablet_meta_table_row.cc
	ob_first_tablet_entry_meta.cc
	ob_first_tablet_entry_schema.cc
	ob_first_tablet_entry.cc
	ob_scan_helper_impl.cc
	ob_meta_table_schema.cc
	ob_meta_table3.cc
	ob_root_table3.cc
	ob_root_table_service.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "roottable")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${ROOTTABLE_SOURCES})
SET(LOCAL_LIBRARIES  ${SYSLIBS} ${EASYLIB} tbsys.static)
BUILD_STATIC_LIBRARY()

