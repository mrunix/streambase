AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbnet \
			  -I${TBLIB_ROOT}/include/tbsys \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src 

LDADD = $(top_builddir)/src/common/libcommon.a \
				$(top_builddir)/src/common/btree/libbtree.a \
				${TBLIB_ROOT}/lib/libtbnet.a \
				${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt  -lnuma
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif


pub_source = \
             ob_ms_schema_task.cpp \
             ob_ms_lease_task.cpp \
             ob_ms_monitor_task.cpp \
             ob_ms_ups_task.cpp \
             ob_merge_server.cpp \
				 		 ob_cell_stream.cpp \
						 ob_ms_service_monitor.cpp \
             ob_merge_server_main.cpp \
						 ob_cell_operator.cpp  \
						 ob_merge_server_params.cpp \
             ob_ms_server_blacklist.cpp \
					 	 ob_ms_rpc_proxy.cpp\
						 ob_merge_join_agent_imp.cpp \
						 ob_merge_server_service.cpp \
						 ob_ms_cache_table.cpp \
						 ob_ms_btreemap.cpp	\
             ob_ms_read_balance.cpp \
						 ob_ms_tablet_location.cpp \
						 ob_ms_schema_manager.cpp \
						 ob_ms_get_cell_stream.cpp\
					 	 ob_ms_scan_cell_stream.cpp \
					 	 ob_ms_cell_stream.cpp \
						 ob_cs_get_cell_stream_wrapper.cpp \
						 ob_read_param_modifier.cpp \
						 ob_ms_tablet_location_item.cpp \
						 ob_ms_cache_table.cpp \
						 ob_row_cell_vec.cpp \
						 ob_groupby_operator.cpp \
						 ob_read_param_decoder.cpp \
						 ob_join_cache.cpp

noinst_LIBRARIES = libmergeserver.a
bin_PROGRAMS = mergeserver
libmergeserver_a_SOURCES= ${pub_source} ob_ms_rpc_stub.cpp ob_merge_join_agent.cpp
mergeserver_SOURCES= main.cpp ${pub_source} ob_ms_rpc_stub.cpp ob_merge_join_agent.cpp
EXTRA_DIST = \
	     ob_cell_operator.h \
	     ob_cell_stream.h \
       ob_cs_get_cell_stream_wrapper.h \
	     ob_merge_join_agent.h \
	     ob_merge_join_agent_imp.h \
	     ob_merge_server.h \
	     ob_merge_server_main.h \
	     ob_merge_server_params.h \
	     ob_ms_server_blacklist.h \
	     ob_merge_server_service.h \
	     ob_ms_stlmap.h \
	     ob_ms_btreemap.h \
	     ob_ms_service_monitor.h \
	     ob_ms_cache_table.h \
	     ob_ms_cell_stream.h \
	     ob_ms_get_cell_stream.h \
	     ob_ms_rpc_proxy.h \
	     ob_ms_rpc_stub.h \
	     ob_ms_scan_cell_stream.h \
	     ob_ms_schema_manager.h \
	     ob_ms_tablet_location.h \
	     ob_ms_tablet_location_item.h \
	     ob_ms_schema_task.h \
       ob_ms_lease_task.h \
       ob_ms_monitor_task.h \
	     ob_ms_ups_task.h \
       ob_ms_read_balance.h \
       ob_read_param_modifier.h \
	     ob_row_cell_vec.h \
	     ob_join_cache.h \
	     ob_groupby_operator.h \
       ob_ms_define.h \
       ob_read_param_decoder.h \
       ob_ms_tsi.h \
	     mergeserver.conf.template

pkgdata_data_DATA=mergeserver.conf.template
pkgdata_datadir=${prefix}/etc
clean-local:
	-rm -f *.gcov *.gcno *.gcda
