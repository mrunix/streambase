
SET(nameserver_lib_sources
	chunk_server_manager.cc
	nameserver.cc
	nameserver_worker.cc
	server_balance_info.cc
	nameserver_log_worker.cc
	tablet_info_manager.cc
	nameserver_meta.cc
	nameserver_table.cc
	nameserver_log_replay.cc
	nameserver_log_manager.cc
	nameserver_rpc_stub.cc
	nameserver_fetch_thread.cc
	nameserver_stat_key.cc
	migrate_info.h migrate_info.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "nameserver")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${nameserver_lib_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	common.static
)
BUILD_STATIC_LIBRARY()


SET(nameserver_sources
	${PROJECT_SOURCE_DIR}/version.cc
	nameserver_main.cc
	main.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "nameserver")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${nameserver_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	nameserver.static
)
BUILD_EXECUTABLE()


SET(schema_reader_sources
	${PROJECT_SOURCE_DIR}/version.cc
	schema_reader.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "schema_reader")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${schema_reader_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	nameserver.static
)
BUILD_EXECUTABLE()


SET(checkpoint2str_sources
	${PROJECT_SOURCE_DIR}/version.cc
	check_point2str.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "checkpoint2str")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${checkpoint2str_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	nameserver.static
)
BUILD_EXECUTABLE()


SET(str2checkpoint_sources
	${PROJECT_SOURCE_DIR}/version.cc
	str2check_point.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "str2checkpoint")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${str2checkpoint_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	nameserver.static
)
BUILD_EXECUTABLE()


SET(rs_admin_sources
	${PROJECT_SOURCE_DIR}/version.cc
	nameserver_admin.cc
	nameserver_admin_main.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "rs_admin")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS} -Wno-sizeof-pointer-memaccess")
SET(LOCAL_CXX_FILES  ${rs_admin_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	nameserver.static
)
BUILD_EXECUTABLE()


SET(rs_stress_sources
	${PROJECT_SOURCE_DIR}/version.cc
	ns_stress.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "rs_stress")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS} -Wno-sizeof-pointer-memaccess")
SET(LOCAL_CXX_FILES  ${rs_stress_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	nameserver.static
)
BUILD_EXECUTABLE()


# Unittest
FUNCTION(BUILD_TEST_APP name)
	SET(${name}_sources
		${ARGN}
	)

	CLEAR_VARS()
	SET(LOCAL_MODULE     "${name}")
	SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS} -Wno-write-strings")
	SET(LOCAL_CXX_FILES  ${${name}_sources})
	SET(LOCAL_LIBRARIES
		tbsys.static
		common.static
		nameserver.static
	)
	BUILD_UNITTEST()
ENDFUNCTION()

IF(WITH_TEST)
 	BUILD_TEST_APP("base_main_test" base_main_test.cc test_main.cc)
	BUILD_TEST_APP("nameserver_test" nameserver_test.cc)
	BUILD_TEST_APP("tablet_info_manager_test" tablet_info_manager_test.cc)
	BUILD_TEST_APP("nameserver_table_test" nameserver_table_test.cc)
	BUILD_TEST_APP("mock_ups_4_root" mock_server.cc mock_update_server.cc test_update_server_main.cc)
	BUILD_TEST_APP("mock_cs_4_root" mock_server.cc mock_chunk_server.cc test_chunk_server_main.cc)
ENDIF()
