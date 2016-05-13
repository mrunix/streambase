
SET(UPDATESERVER_FLAGS  "${COMMON_CXXFLAGS} -D__VERSION_ID__=\"updateserver 1.0.0.0\" -D_BTREE_ENGINE_ -Wno-error=psabi")

SET(updateserver_lib_sources
	${PROJECT_SOURCE_DIR}/svn_version.cc
	ob_async_log_applier.cc
	ob_btree_engine_alloc.cc
	ob_cached_pos_log_reader.cc
	ob_data_block.cc
	ob_fetched_log.cc
	ob_located_log_reader.cc
	ob_lock_mgr.cc
	ob_log_buffer.cc
	ob_log_replay_worker.cc
	ob_log_src.cc
	ob_log_sync_delay_stat.cc
	ob_memtable.cc
	ob_memtable_rowiter.cc
	ob_multi_file_utils.cc
	ob_on_disk_log_locator.cc
	ob_pos_log_reader.cc
	ob_prefetch_log_buffer.cc
	ob_query_engine.cc
	ob_remote_log_src.cc
	ob_replay_log_src.cc
	ob_ring_data_buffer.cc
	ob_schema_mgrv2.cc
	ob_session_mgr.cc
	ob_sessionctx_factory.cc
	ob_slave_sync_type.cc
	ob_sstable_mgr.cc
	ob_store_mgr.cc
	ob_table_engine.cc
	ob_table_mgr.cc
	ob_trans_buffer.cc
	ob_trans_executor.cc
	ob_trans_mgr.cc
	ob_transfer_sstable_query.cc
	ob_update_callback.cc
	ob_update_server.cc
	ob_update_server_main.cc
	ob_ups_cache.cc
	ob_ups_clog_status.cc
	ob_ups_compact_cell_iterator.cc
	ob_ups_compact_cell_writer.cc
	ob_ups_fetch_runnable.cc
	ob_ups_log_mgr.cc
	ob_ups_log_utils.cc
	ob_ups_mutator.cc
	ob_ups_replay_runnable.cc
	ob_ups_rpc_stub.cc
	ob_ups_slave_mgr.cc
	ob_ups_stat.cc
	ob_ups_table_mgr.cc
	ob_ups_timer_task.cc
	ob_ups_utils.cc
	ob_update_server_config.cc
	ob_ups_phy_operator_factory.cc
	ob_table_list_query.cc
	ob_ups_lock_filter.cc
	ob_ups_inc_scan.cc
	ob_clog_stat.cc
	ob_memtable_modify.cc
	ob_trigger_handler.cc
	ob_update_reload_config.cc
	ob_inc_seq.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "updateserver")
SET(LOCAL_CXX_FLAGS  "${UPDATESERVER_FLAGS}")
SET(LOCAL_CXX_FILES  ${updateserver_lib_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	sql.static
	sstable.static
	comp.static
	common.static
	btree.static
	timezone.static
)
BUILD_STATIC_LIBRARY()


SET(updateserver_sources
	main.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "updateserver")
SET(LOCAL_CXX_FLAGS  "${UPDATESERVER_FLAGS}")
SET(LOCAL_CXX_FILES  ${updateserver_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	updateserver.static
)
BUILD_EXECUTABLE()


SET(ob_ping_sources
	${PROJECT_SOURCE_DIR}/svn_version.cc
	ups_mon.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "ob_ping")
SET(LOCAL_CXX_FLAGS  "${UPDATESERVER_FLAGS}")
SET(LOCAL_CXX_FILES  ${ob_ping_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	updateserver.static
)
BUILD_EXECUTABLE()

