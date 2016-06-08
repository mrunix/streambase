
SET(UPDATESERVER_FLAGS  "${COMMON_CXXFLAGS} -D__VERSION_ID__=\"updateserver 1.0.0.0\" -D_BTREE_ENGINE_ -Wno-error=psabi")

SET(updateserver_lib_sources
	${PROJECT_SOURCE_DIR}/version.cc
	ob_hash_engine.cc
    ob_trans_buffer.cc
    ob_memtable.cc
    ob_ups_mutator.cc
    ob_schema_mgr.cc
    ob_ups_table_mgr.cc
    ob_ups_rpc_stub.cc
    ob_update_server.cc
    ob_update_server_main.cc
    ob_update_server_param.cc
    ob_ups_utils.cc
    ob_ups_log_mgr.cc
    ob_btree_engine.cc
    ob_btree_engine_alloc.cc
    ob_store_mgr.cc
    ob_sstable_mgr.cc
    ob_table_mgr.cc
    ob_memtable_rowiter.cc
    ob_ups_replay_runnable.cc
    ob_transfer_sstable_query.cc
    ob_ups_fetch_runnable.cc
    ob_multi_file_utils.cc
    ob_schema_mgrv2.cc
    ob_commit_log_receiver.cc
    ob_ups_fetch_lsync.cc
    ob_ups_stat.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "updateserver")
SET(LOCAL_CXX_FLAGS  "${UPDATESERVER_FLAGS}")
SET(LOCAL_CXX_FILES  ${updateserver_lib_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	tbnet.static
	sstable.static
	comp.static
	common.static
	btree.static
	chunkserver.static
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
	tbsys.static
	updateserver.static
)
BUILD_EXECUTABLE()


SET(ups_mon_sources
	${PROJECT_SOURCE_DIR}/version.cc
	ups_mon.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "ups_mon")
SET(LOCAL_CXX_FLAGS  "${UPDATESERVER_FLAGS}")
SET(LOCAL_CXX_FILES  ${ups_mon_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	updateserver.static
)
BUILD_EXECUTABLE()

