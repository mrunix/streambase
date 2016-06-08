AM_CPPFLAGS= -I${TBLIB_ROOT}/include/tbsys \
			 -I${TBLIB_ROOT}/include/tbnet \
			 -I${top_srcdir}/include \
			 -I${top_srcdir}/src

LDADD = ${TBLIB_ROOT}/lib/libtbnet.a \
				${TBLIB_ROOT}/lib/libtbsys.a
#CXXFLAGS+=-O2 -Wno-strict-aliasing
AM_LDFLAGS=-lpthread -lc -lm  -lrt -lnuma #-pg
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libcommon.a

libcommon_a_SOURCES = dlist.cpp \
		      ob_object.cpp \
		      ob_timer.cpp \
		      ob_server.cpp \
		      ob_schema.cpp \
		      file_utils.cpp \
		      file_directory_utils.cpp \
          murmur_hash.cpp \
		      ob_merger.cpp \
		      ob_range.cpp \
		      ob_string_buf.cpp\
		      ob_tablet_info.cpp \
		      base_main.cpp \
		      ob_base_server.cpp \
		      ob_single_server.cpp \
		      ob_simple_filter.cpp \
		      ob_simple_condition.cpp \
		      ob_probability_random.cpp \
          ob_cell_array.cpp \
		      ob_scanner.cpp \
		      ob_scan_param.cpp \
		      ob_common_param.cpp \
		      ob_get_param.cpp \
		      ob_memory_pool.cpp \
		      ob_malloc.cpp \
		      wait_object.cpp \
		      ob_packet.cpp \
		      thread_buffer.cpp \
		      ob_client_manager.cpp \
		      ob_cache.cpp \
		      ob_trace_log.cpp \
		      ob_record_header.cpp \
		      utility.cpp \
		      ob_mutator.cpp \
		      ob_log_replay_runnable.cpp \
		      ob_log_writer.cpp \
		      ob_log_reader.cpp \
		      ob_single_log_reader.cpp \
		      ob_log_dir_scanner.cpp \
		      ob_fetch_runnable.cpp \
		      ob_check_runnable.cpp \
		      ob_slave_mgr.cpp \
		      ob_log_entry.cpp \
		      ob_common_rpc_stub.cpp \
		      batch_packet_queue_thread.cpp \
		      ob_mod_define.cpp \
		      ob_crc64.cpp \
		      ob_lease_common.cpp \
		      ob_packet_queue_thread.cpp \
		      ob_thread_mempool.cpp \
		      ob_tsi_factory.cpp \
		      ob_statistics.cpp \
		      ob_slab.cpp \
		      ob_regex.cpp \
		      ob_string_search.cpp \
		      ob_groupby.cpp \
					ob_update_condition.cpp\
					ob_shadow_server.cpp\
					priority_packet_queue_thread.cpp \
          ob_packet_queue.cpp \
          ob_file.cpp \
          ob_ring_buffer.cpp\
		  ob_obi_role.cpp\
		  ob_base_client.cpp\
			ob_obi_config.cpp\
			ob_ups_info.h ob_ups_info.cpp\
      ob_row_compaction.cpp\
			ob_client_config.h ob_client_config.cpp

bin_PROGRAMS = ob_ping
ob_ping_SOURCES = ob_ping.cpp ${libcommon_a_SOURCES}

SUBDIRS=btree compress
DIST_SUBDIRS=btree compress

EXTRA_DIST = \
	base_main.h \
	batch_packet_queue_thread.h \
	bloom_filter.h \
	check_sum.h \
	ob_groupby.h \
	data_buffer.h \
	dlist.h \
	file_directory_utils.h \
	file_utils.h \
	limit_array.h \
	murmur_hash.h \
	ob_action_flag.h \
	ob_array_helper.h \
	ob_base_server.h \
	ob_cache.h \
	ob_check_runnable.h \
	ob_client_manager.h \
	ob_cell_array.h \
	ob_simple_condition.h \
	ob_simple_filter.h \
	ob_common_param.h \
	ob_common_rpc_stub.h \
	ob_define.h \
	ob_endian.h \
	ob_fetch_runnable.h \
	ob_get_param.h \
	ob_iterator.h \
	ob_link.h \
	ob_log_dir_scanner.h \
	ob_log_entry.h \
	ob_log_reader.h \
	ob_log_replay_runnable.h \
	ob_log_writer.h \
	ob_lrucache.h \
	ob_malloc.h \
	ob_memory_pool.h \
	ob_merger.h \
	ob_mutator.h \
	ob_object.h \
	ob_operate_result.h \
	ob_packet.h \
	ob_packet_factory.h \
	ob_packet_queue_thread.h \
	ob_range.h \
	ob_read_common_data.h \
	ob_record_header.h \
	ob_result.h \
	ob_role_mgr.h \
	ob_scanner.h \
	ob_scan_param.h \
	ob_schema.h \
	ob_server.h \
	ob_single_log_reader.h \
	ob_single_server.h \
	ob_slave_mgr.h \
	ob_string.h \
	ob_string_buf.h \
	ob_tablet_info.h \
	ob_timer.h \
	ob_trace_log.h \
	ob_vector.h \
	page_arena.h \
	serialization.h \
	thread_buffer.h \
	utility.h \
	wait_object.h \
	hash/ob_hashmap.h \
	hash/ob_hashset.h \
	hash/ob_hashtable.h \
	hash/ob_hashutils.h \
	hash/ob_serialization.h \
	ob_vector.ipp \
	ob_statistics.h \
	ob_mod_define.h \
	ob_crc64.h \
	ob_atomic.h \
	ob_lease_common.h \
	ob_thread_mempool.h \
	ob_tsi_factory.h \
	ob_ping.h \
	ob_bloomfilter.h \
	ob_slab.h \
	ob_bloomfilter.h \
  ob_string_search.h  \
	ob_update_condition.h \
	ob_shadow_server.h\
  ob_probability_random.h \
  priority_packet_queue_thread.h \
  ob_packet_queue.h \
  ob_ring_buffer.h \
  ob_regex.h \
  ob_spin_rwlock.h \
	ob_fileinfo_manager.h \
  ob_kv_storecache.h \
	ob_list.h \
	ob_thread_objpool.h \
	ob_bitmap.h \
  ob_file.h\
	ob_obi_role.h\
	ob_base_client.h\
	ob_obi_config.h\
  ob_row_compaction.h \
  ob_log_cursor.h
clean-local:
	-rm -f *.gcov *.gcno *.gcda
