
SET(obsql_sources
	ob_sql_atomic.cc
	ob_sql_curl.cc
	ob_sql_cluster_config.cc
	ob_sql_update_worker.cc
	ob_sql_data_source_utility.cc
	ob_sql_group_data_source.cc
	ob_sql_init.cc
	ob_sql_conn_acquire.cc
	ob_sql_util.cc
	ob_sql_real_func.cc
	ob_sql_cluster_select.cc
	ob_sql_ms_select.cc
	ob_sql_mysql_adapter.cc
	ob_sql_list.cc
	ob_sql_select_method_impl.cc
	ob_sql_global.cc
	ob_sql_conn_recycle.cc
	ob_sql_parser.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "obsql")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS} -Wl,-static,-ltbnet -Wl,-static,-ltbsys -Wl,-e,so_main -Wno-error=array-bounds")
SET(LOCAL_CXX_FILES  ${obsql_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	common.static
	rootserver.static
)
BUILD_LIBRARY()

