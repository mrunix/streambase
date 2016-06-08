
SET(chunkserver_lib_sources
	${PROJECT_SOURCE_DIR}/version.cc
	ob_chunk_server.cc
	ob_chunk_server_main.cc
	ob_chunk_server_param.cc
	ob_chunk_service.cc
	ob_fileinfo_cache.cc
	ob_tablet_manager.cc
	ob_root_server_rpc.cc
	ob_tablet.cc
	ob_tablet_image.cc
	ob_merge_reader.cc
	ob_disk_manager.cc
	ob_file_recycle.cc
	ob_chunk_merge.cc
	ob_chunk_server_merger_proxy.cc
	ob_block_cache_reader.cc
	ob_block_cache_loader.cc
	ob_switch_cache_utility.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "chunkserver")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${chunkserver_lib_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	tbnet.static
	sstable.static
	common.static
	comp.static
	btree.static
	mergeserver.static
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
	tbsys.static
	tbnet.static
	sstable.static
	common.static
	comp.static
	btree.static
	mergeserver.static
)
BUILD_EXECUTABLE()

