
INCLUDE_DIRECTORIES(
	${PROJECT_SOURCE_DIR}/tools/getquerytest/include/mysql
)

SET(getquery_sources
	getquery.cc
	get_query_test.cc
	ob_sql_client.cc
	ob_sql_generator.cc
	ob_sql_db_creater.cc
	ob_sql_writer.cc
	ob_sql_reader.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "getquery")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS} -D__STDC_LIMIT_MACROS -DORDER_SERIALIZE")
SET(LOCAL_CXX_FILES  ${getquery_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	common.static
	mysqlclient
)
BUILD_EXECUTABLE()

