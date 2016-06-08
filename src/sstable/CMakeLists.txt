
SET(sstable_sources
	ob_blockcache.cc
	ob_sstable_reader.cc
	ob_sstable_block_index_v2.cc
	ob_block_index_cache.cc
	ob_sstable_scan_param.cc
	ob_sstable_scanner.cc
	ob_sstable_schema.cc
	ob_sstable_schema_cache.cc
	ob_sstable_row.cc
	ob_sstable_block_builder.cc
	ob_sstable_block_index_builder.cc
	ob_sstable_block_index_buffer.cc
	ob_sstable_writer.cc
	ob_sstable_trailer.cc
	ob_sstable_block_reader.cc
	ob_sstable_block_scanner.cc
	ob_scan_column_indexes.cc
	ob_seq_sstable_scanner.cc
	ob_column_group_scanner.cc
	ob_sstable_block_getter.cc
	ob_sstable_getter.cc
	ob_aio_buffer_mgr.cc
	ob_aio_event_mgr.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "sstable")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${sstable_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	comp.static
	common.static
)
BUILD_STATIC_LIBRARY()

