
SET(OBLOG_FLAGS  "${COMMON_CXXFLAGS} -D__DENSE_AVAILABLE__")
SET(OBLOG_FLAGS  "${OBLOG_FLAGS} -rdynamic")
SET(OBLOG_FLAGS  "${OBLOG_FLAGS} -Wl,-Bsymbolic -Wl,-e,so_main")

INCLUDE_DIRECTORIES(
	${PROJECT_SOURCE_DIR}/external/ds/include/drcmessage
	/usr/include/lua5.1
)

SET(oblog_cxx_sources
	liboblog.cc
	MetaData.cc
	BinlogRecord.cc
	ob_log_rpc_stub.cc
	ob_log_filter.cc
	ob_log_router.cc
	ob_log_meta_manager.cc
	ob_log_formator.cc
	ob_log_fetcher.cc
	ob_log_spec.cc
	ob_log_config.cc
	ob_log_entity.cc
	ob_log_partitioner.cc
	ob_log_server_selector.cc
	ob_log_mysql_adaptor.cc
)

SET(oblog_c_sources
	ob_log_main.c
	${PROJECT_SOURCE_DIR}/version.c
)


CLEAR_VARS()
SET(LOCAL_MODULE     "oblog")
SET(LOCAL_C_FLAGS    "${OBLOG_FLAGS}")
SET(LOCAL_CXX_FLAGS  "${OBLOG_FLAGS} -Wno-error=psabi") # ob_priority_scheduler.h:34:33: error: AVX vector return without AVX enabled changes the ABI [-Werror=psabi]
SET(LOCAL_C_FILES    ${oblog_c_sources})
SET(LOCAL_CXX_FILES  ${oblog_cxx_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	common.static
	sstable.static
	comp.static
	curl
	lua5.1
	${PROJECT_SOURCE_DIR}/external/ds/lib64/libdrcmsg.so
)
BUILD_LIBRARY()

