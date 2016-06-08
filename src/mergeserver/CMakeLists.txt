
SET(mergeserver_lib_sources
	ob_ms_schema_task.cc
	ob_ms_lease_task.cc
	ob_ms_monitor_task.cc
	ob_ms_ups_task.cc
	ob_merge_server.cc
	ob_cell_stream.cc
	ob_ms_service_monitor.cc
	ob_merge_server_main.cc
	ob_cell_operator.cc
	ob_merge_server_params.cc
	ob_ms_server_blacklist.cc
	ob_ms_rpc_proxy.cc
	ob_merge_join_agent_imp.cc
	ob_merge_server_service.cc
	ob_ms_cache_table.cc
	ob_ms_btreemap.cc
	ob_ms_read_balance.cc
	ob_ms_tablet_location.cc
	ob_ms_schema_manager.cc
	ob_ms_get_cell_stream.cc
	ob_ms_scan_cell_stream.cc
	ob_ms_cell_stream.cc
	ob_cs_get_cell_stream_wrapper.cc
	ob_read_param_modifier.cc
	ob_ms_tablet_location_item.cc
	ob_ms_cache_table.cc
	ob_row_cell_vec.cc
	ob_groupby_operator.cc
	ob_read_param_decoder.cc
	ob_join_cache.cc

	ob_ms_rpc_stub.cc
	ob_merge_join_agent.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "mergeserver")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${mergeserver_lib_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	tbnet.static
	common.static
	btree.static
)
BUILD_STATIC_LIBRARY()


SET(mergeserver_sources
	${PROJECT_SOURCE_DIR}/version.cc
	main.cc
	ob_ms_rpc_stub.cc
	ob_merge_join_agent.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "mergeserver")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${mergeserver_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	common.static
	mergeserver.static
)
BUILD_EXECUTABLE()
