
SET(chunkserver_lib_sources
	${PROJECT_SOURCE_DIR}/svn_version.cc
	ob_block_cache_loader.cc
	ob_block_cache_reader.cc
	ob_cell_array_helper.cc
	ob_cell_stream.cc
	ob_chunk_callback.cc
	ob_chunk_merge.cc
	ob_tablet_merger_v1.cc
	ob_tablet_merger_v2.cc
	ob_chunk_server.cc
	ob_chunk_server_main.cc
	ob_chunk_server_merger_proxy.cc
	ob_chunk_server_stat.cc
	ob_chunk_service.cc
	ob_compactsstable_cache.cc
	ob_disk_manager.cc
	ob_file_recycle.cc
	ob_fileinfo_cache.cc
	ob_get_cell_stream.cc
	ob_get_cell_stream_wrapper.cc
	ob_get_param_cell_iterator.cc
	ob_get_scan_proxy.cc
	ob_join_cache.cc
	ob_join_operator.cc
	ob_merge_join_operator.cc
	ob_merge_operator.cc
	ob_merge_reader.cc
	ob_multi_tablet_merger.cc
	ob_query_agent.cc
	ob_query_service.cc
	ob_read_param_modifier.cc
	ob_read_ups_balance.cc
	ob_row_cell_vec.cc
	ob_rpc_proxy.cc
	ob_scan_cell_stream.cc
	ob_schema_task.cc
	ob_sql_rpc_stub.cc
	ob_switch_cache_utility.cc
	ob_tablet.cc
	ob_tablet_image.cc
	ob_tablet_manager.cc
	ob_tablet_merge_filter.cc
	ob_ups_blacklist.cc
	ob_chunk_server_config.cc
	ob_bypass_sstable_loader.cc
	ob_tablet_service.cc
	ob_tablet_writer.cc
	ob_tablet_reader.cc
	ob_chunk_reload_config.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "chunkserver")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${chunkserver_lib_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	sstable.static
	compactsstable.static
	common.static
	sql.static
	compactsstablev2.static
	comp.static
	btree.static
	timezone.static
)
BUILD_STATIC_LIBRARY()


SET(chunkserver_sources
	ob_disk_path.cc
	main.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "chunkserver")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${chunkserver_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	sstable.static
	compactsstable.static
	common.static
	sql.static
	compactsstablev2.static
	comp.static
	btree.static
	timezone.static
	chunkserver.static
)
BUILD_EXECUTABLE()

