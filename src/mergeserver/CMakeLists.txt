
SET(mergeserver_lib_sources
	ob_chunk_server_task_dispatcher.cc
	ob_merge_callback.cc
	ob_merge_server.cc
	ob_merge_server_main.cc
	ob_merge_server_service.cc
	ob_merger_groupby_operator.cc
	ob_ms_server_counter.cc
	ob_merger_operator.cc
	ob_merger_reverse_operator.cc
	ob_merger_sorted_operator.cc
	ob_ms_lease_task.cc
	ob_ms_monitor_task.cc
	ob_ms_rpc_proxy.cc
	ob_ms_async_rpc.cc
	ob_rpc_event.cc
	ob_ms_rpc_event.cc
	ob_ms_request.cc
	ob_ms_scan_request.cc
	ob_ms_sub_scan_request.cc
	ob_ms_get_request.cc
	ob_ms_sub_get_request.cc
	ob_ms_scan_param.cc
	ob_ms_scanner_encoder.cc
	ob_ms_schema_proxy.cc
	ob_ms_schema_task.cc
	ob_ms_service_monitor.cc
	ob_ms_sql_sorted_operator.cc
	ob_ms_sql_operator.cc
	ob_sql_rpc_event.cc
	ob_ms_sql_rpc_event.cc
	ob_ms_sql_request.cc
	ob_ms_sql_scan_request.cc
	ob_ms_sql_sub_scan_request.cc
	ob_ms_sql_get_request.cc
	ob_ms_sql_sub_get_request.cc
	ob_ms_ups_task.cc
	ob_mutator_param_decoder.cc
	ob_param_decoder.cc
	ob_query_cache.cc
	ob_read_param_decoder.cc
	ob_read_param_modifier.cc
	ob_rs_rpc_proxy.cc
	ob_merge_server_config.cc
	ob_ms_sql_proxy.cc
	ob_get_privilege_task.cc
	ob_bloom_filter_task_queue_thread.cc
	ob_insert_cache.cc
	ob_merge_reload_config.cc
	ob_frozen_data_cache.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "mergeserver")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${mergeserver_lib_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	sql.static
	obmysql.static
	common.static
	btree.static
	timezone.static
)
BUILD_STATIC_LIBRARY()


SET(mergeserver_sources
	${PROJECT_SOURCE_DIR}/svn_version.cc
	main.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "mergeserver")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${mergeserver_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	sql.static
	obmysql.static
	common.static
	timezone.static
	mergeserver.static
	chunkserver.static
)
BUILD_EXECUTABLE()
