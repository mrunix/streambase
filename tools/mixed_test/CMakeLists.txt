
SET(MIXED_TEST_LIBRARIES
	${SYSLIBS}
	tbsys.static
	common.static
)

INCLUDE_DIRECTORIES(
	${PROJECT_SOURCE_DIR}/tests/common
	${PROJECT_SOURCE_DIR}/tests/updateserver
)

SET(MIXED_TEST_FLAGS  "${COMMON_CXXFLAGS} -D_BTREE_ENGINE_")

SET(updateserver_sources
	mutator_builder.cc
	rowkey_builder.cc
	utils.cc
	cellinfo_builder.cc
	client_wrapper.cc
	row_checker.cc
	${PROJECT_SOURCE_DIR}/tests/updateserver/test_utils.cc
	${PROJECT_SOURCE_DIR}/src/updateserver/ob_ups_stat.cc
)


SET(multi_write_sources
	multi_write.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "multi_write")
SET(LOCAL_CXX_FLAGS  "${MIXED_TEST_FLAGS}")
SET(LOCAL_CXX_FILES  ${multi_write_sources} ${updateserver_sources})
SET(LOCAL_LIBRARIES  ${MIXED_TEST_LIBRARIES} updateserver.static)
BUILD_EXECUTABLE()


SET(total_scan_sources
	total_scan.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "total_scan")
SET(LOCAL_CXX_FLAGS  "${MIXED_TEST_FLAGS}")
SET(LOCAL_CXX_FILES  ${total_scan_sources} ${updateserver_sources})
SET(LOCAL_LIBRARIES  ${MIXED_TEST_LIBRARIES} updateserver.static)
BUILD_EXECUTABLE()


SET(random_read_sources
	random_read.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "random_read")
SET(LOCAL_CXX_FLAGS  "${MIXED_TEST_FLAGS}")
SET(LOCAL_CXX_FILES  ${random_read_sources} ${updateserver_sources})
SET(LOCAL_LIBRARIES  ${MIXED_TEST_LIBRARIES} updateserver.static)
BUILD_EXECUTABLE()


SET(launcher_sources
	launcher.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "launcher")
SET(LOCAL_CXX_FLAGS  "${MIXED_TEST_FLAGS}")
SET(LOCAL_CXX_FILES  ${launcher_sources})
SET(LOCAL_LIBRARIES  ${MIXED_TEST_LIBRARIES})
BUILD_EXECUTABLE()

