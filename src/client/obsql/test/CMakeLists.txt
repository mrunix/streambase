
SET(OBSQL_TEST_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(OBSQL_TEST_LIBRARIES
	${SYSLIBS}
	tbsys.static
	obsql${LIB_TYPE}
	common.static
	mysqlclient
	curl
)

INCLUDE_DIRECTORIES(
	${PROJECT_SOURCE_DIR}/src/client/obsql/src
)

SET(sqltest_sources
	ob_sql_test.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "sqltest")
SET(LOCAL_CXX_FLAGS  "${OBSQL_TEST_CXX_FLAGS}")
SET(LOCAL_CXX_FILES  ${sqltest_sources})
SET(LOCAL_LIBRARIES  ${OBSQL_TEST_LIBRARIES})
BUILD_EXECUTABLE()


SET(msqltest_sources
	ob_sql_test_multh.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "msqltest")
SET(LOCAL_CXX_FLAGS  "${OBSQL_TEST_CXX_FLAGS}")
SET(LOCAL_CXX_FILES  ${msqltest_sources})
SET(LOCAL_LIBRARIES  ${OBSQL_TEST_LIBRARIES})
BUILD_EXECUTABLE()


SET(whiletest_sources
	ob_sql_while_test.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "whiletest")
SET(LOCAL_CXX_FLAGS  "${OBSQL_TEST_CXX_FLAGS}")
SET(LOCAL_CXX_FILES  ${whiletest_sources})
SET(LOCAL_LIBRARIES  ${OBSQL_TEST_LIBRARIES})
BUILD_EXECUTABLE()



