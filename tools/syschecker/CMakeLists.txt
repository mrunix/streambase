ADD_SUBDIRECTORY(client)

SET(syschecker_sources
	main.cc
	ob_syschecker_main.cc
	ob_syschecker.cc
	ob_syschecker_param.cc
	ob_write_worker.cc
	ob_read_worker.cc
	ob_syschecker_stat.cc
	ob_syschecker_rule.cc
	ob_syschecker_schema.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "syschecker")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${syschecker_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	common.static
	client.static
	chunkserver.static
)
BUILD_EXECUTABLE()

