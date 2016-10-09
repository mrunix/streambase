
ADD_SUBDIRECTORY(btree)
ADD_SUBDIRECTORY(compress)

SET(common_sources
	dlist.cc
	ob_object.cc
	ob_timer.cc
	ob_server.cc
	ob_schema.cc
	file_utils.cc
	file_directory_utils.cc
	murmur_hash.cc
	ob_merger.cc
	ob_range.cc
	ob_string_buf.cc
	ob_tablet_info.cc
	base_main.cc
	ob_base_server.cc
	ob_single_server.cc
	ob_simple_filter.cc
	ob_simple_condition.cc
	ob_probability_random.cc
	ob_cell_array.cc
	ob_scanner.cc
	ob_scan_param.cc
	ob_common_param.cc
	ob_get_param.cc
	ob_memory_pool.cc
	ob_malloc.cc
	wait_object.cc
	ob_packet.cc
	thread_buffer.cc
	ob_client_manager.cc
	ob_cache.cc
	ob_trace_log.cc
	ob_record_header.cc
	utility.cc
	ob_mutator.cc
	ob_log_replay_runnable.cc
	ob_log_writer.cc
	ob_log_reader.cc
	ob_single_log_reader.cc
	ob_log_dir_scanner.cc
	ob_fetch_runnable.cc
	ob_check_runnable.cc
	ob_slave_mgr.cc
	ob_log_entry.cc
	ob_common_rpc_stub.cc
	batch_packet_queue_thread.cc
	ob_mod_define.cc
	ob_crc64.cc
	ob_lease_common.cc
	ob_packet_queue_thread.cc
	ob_thread_mempool.cc
	ob_tsi_factory.cc
	ob_statistics.cc
	ob_slab.cc
	ob_regex.cc
	ob_string_search.cc
	ob_groupby.cc
	ob_update_condition.cc
	ob_shadow_server.cc
	priority_packet_queue_thread.cc
	ob_packet_queue.cc
	ob_file.cc
	ob_ring_buffer.cc
	ob_obi_role.cc
	ob_base_client.cc
	ob_obi_config.cc
	ob_ups_info.h ob_ups_info.cc
	ob_row_compaction.cc
	ob_client_config.h ob_client_config.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "common")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")	# ob_priority_scheduler.h:34:33: error: AVX vector return without AVX enabled changes the ABI [-Werror=psabi]
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS} -Wno-strict-aliasing")	# ob_priority_scheduler.h:34:33: error: AVX vector return without AVX enabled changes the ABI [-Werror=psabi]
SET(LOCAL_CXX_FILES  ${common_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
	tbnet.static
	btree.static
)
BUILD_STATIC_LIBRARY()

