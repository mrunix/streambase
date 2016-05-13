
INCLUDE_DIRECTORIES(
	${PROJECT_SOURCE_DIR}/tools/dump
)

SET(ob_import_sources
	tokenizer.cc
	ob_import.cc
	ob_import_producer.cc
	db_utils.cc
	file_reader.cc
	oceanbase_db.cc
	db_record_set.cc
	ob_import_comsumer.cc
	ob_import_param.cc
	ob_import_main.cc
	db_table_info.cc
	db_record.cc
	${PROJECT_SOURCE_DIR}/src/updateserver/ob_ups_stat.cc
	file_appender.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "ob_import")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${ob_import_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	common.static
	updateserver.static
)
BUILD_EXECUTABLE()

