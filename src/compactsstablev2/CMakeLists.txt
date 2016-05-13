
SET(compactsstablev2_sources
	ob_sstable_buffer.cc
	ob_sstable_block_builder.cc
	ob_sstable_block_endkey_builder.cc
	ob_sstable_block_index_builder.cc
	ob_sstable_table_index_builder.cc
	ob_sstable_table_schema_builder.cc
	ob_sstable_table_range_builder.cc
	ob_sstable_table.cc
	ob_sstable_schema.cc
	ob_sstable.cc
	ob_compact_sstable_writer_buffer.cc
	ob_compact_sstable_writer.cc
	ob_sstable_block_index_mgr.cc
	ob_sstable_block_index_cache.cc
	ob_sstable_aio_event_mgr.cc
	ob_sstable_aio_buffer_mgr.cc
	ob_sstable_block_cache.cc
	ob_sstable_schema_cache.cc
	ob_compact_sstable_reader.cc
	ob_sstable_block_reader.cc
	ob_sstable_block_scanner.cc
	ob_sstable_scan_column_indexes.cc
	ob_compact_sstable_scanner.cc
	ob_sstable_block_getter.cc
	ob_compact_sstable_getter.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "compactsstablev2")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS} -pg -D_SSTABLE_NO_STAT_")
SET(LOCAL_CXX_FILES  ${compactsstablev2_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	common.static
	sstable.static
)
BUILD_STATIC_LIBRARY()

