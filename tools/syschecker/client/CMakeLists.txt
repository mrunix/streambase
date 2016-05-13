
SET(client_sources
	ob_client.cc
	ob_server_manager.cc
	ob_server_rpc.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "client")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${client_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	common.static
)
BUILD_STATIC_LIBRARY()

