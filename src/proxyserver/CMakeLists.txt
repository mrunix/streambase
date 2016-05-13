SET(PROXYSERVER_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	sstable.static
	sql.static
	common.static
	compactsstable.static
	compactsstablev2.static
	comp.static
	btree.static
)

SET(proxyserver_lib_sources
	ob_proxy_server.cc
	ob_proxy_server_main.cc
	ob_proxy_service.cc
	ob_proxy_server_config.cc
	ob_yunti_proxy.cc
	ob_yunti_meta.cc
	ob_proxy_callback.cc
	ob_proxy_reader.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "proxyserver")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${proxyserver_lib_sources})
SET(LOCAL_LIBRARIES  ${PROXYSERVER_LIBRARIES})
BUILD_STATIC_LIBRARY()


SET(proxyserver_sources
	${PROJECT_SOURCE_DIR}/svn_version.cc
	main.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "proxyserver")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${proxyserver_sources})
SET(LOCAL_LIBRARIES  ${PROXYSERVER_LIBRARIES} proxyserver.static)
BUILD_EXECUTABLE()

