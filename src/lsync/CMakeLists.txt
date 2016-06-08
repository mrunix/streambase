
SET(LSYNCSERVER_CXX_FLAGS  "${COMMON_CXXFLAGS} -DORDER_SERIALIZE -DCOMPATIBLE -D__LSYNC_DEBUG__")

SET(lsyncserver_lib_sources
	ob_lsync_server_param.cc
	ob_lsync_server.cc
	ob_seekable_log_reader.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "lsyncserver")
SET(LOCAL_CXX_FLAGS  "${LSYNCSERVER_CXX_FLAGS}")
SET(LOCAL_CXX_FILES  ${lsyncserver_lib_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	nameserver.static
	common.static
	comp.static
	btree.static
)
BUILD_STATIC_LIBRARY()


SET(lsyncserver_sources
	${PROJECT_SOURCE_DIR}/version.cc
	main.cc
	ob_lsync_server_main.cc
	ob_lsync_server_param.cc
	ob_lsync_server.cc
	ob_seekable_log_reader.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "lsyncserver")
SET(LOCAL_CXX_FLAGS  "${LSYNCSERVER_CXX_FLAGS} -Wno-write-strings")
SET(LOCAL_CXX_FILES  ${lsyncserver_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	lsyncserver.static
)
BUILD_EXECUTABLE()

