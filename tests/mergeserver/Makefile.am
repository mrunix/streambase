AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/btree \
			  -I${top_srcdir}/src/mergeserver 

LDADD = $(top_builddir)/src/mergeserver/libmergeserver.a \
				$(top_builddir)/src/common/libcommon.a \
				$(top_builddir)/src/common/btree/libbtree.a \
				${TBLIB_ROOT}/lib/libtbnet.a \
				${TBLIB_ROOT}/lib/libtbsys.a

CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -D_MS_MOCK_WHOLE_  -D_BTREE_ENGINE_ -D__UNIT_TEST__
AM_LDFLAGS = -lpthread -lc -lm  -lgtest -lrt ${GCOV_LIB}  -lnuma
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

pub_source = mock_server.cpp \
						 mock_root_server.cpp \
						 mock_update_server.cpp \
						 mock_chunk_server.cpp

noinst_PROGRAMS = test_ups_client \
               test_ms_client \
               monitor_tool \
               test_merger_stlmap \
               test_merger_btreemap \
               test_merger_blacklist \
               test_merger_tablet_location \
			         test_merger_schema_manager \
			         test_merger_tablet_location_item \
			         test_merger_timer_task \
			         test_cell_operator \
			         test_merger_rpc_stub \
			         test_merger_rpc_proxy \
			         test_groupby_operator \
			         test_read_param_decoder\
			         test_read_param_modifier \
               test_param_modifier \
               test_scan_backward \
			         test_mj_client \
			         test_special_mj\
			         test_data_build

test_ms_client_SOURCES = test_ms_client.cpp
test_ups_client_SOURCES = test_ups_client.cpp
monitor_tool_SOURCES = monitor_tool.cpp
test_merger_timer_task_SOURCES = test_merger_timer_task.cpp ${pub_source}
test_merger_rpc_proxy_SOURCES = test_merger_rpc_proxy.cpp ${pub_source}
test_merger_rpc_stub_SOURCES = test_merger_rpc_stub.cpp ${pub_source}
test_merger_stlmap_SOURCES = test_merger_stlmap.cpp
test_merger_blacklist_SOURCES = test_merger_blacklist.cpp
test_merger_btreemap_SOURCES = test_merger_btreemap.cpp
test_merger_tablet_location_SOURCES = test_merger_tablet_location.cpp
test_merger_schema_manager_SOURCES = test_merger_schema_manager.cpp
test_merger_tablet_location_item_SOURCES = test_merger_tablet_location_item.cpp
test_groupby_operator_SOURCES = test_groupby_operator.cpp
test_read_param_decoder_SOURCES = test_read_param_decoder.cpp
test_read_param_modifier_SOURCES = test_read_param_modifier.cpp
test_param_modifier_SOURCES = test_param_modifier.cpp
test_scan_backward_SOURCES = test_scan_backward.cpp
test_mj_client_SOURCES=test_mj_client.cpp
test_special_mj_SOURCES=test_special_mj.cpp
test_data_build_SOURCES=test_data_build.cpp
test_cell_operator_SOURCES=test_cell_operator.cpp

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS) 
CLEANFILES = $(check_SCRIPTS)
EXTRA_DIST=mock_chunk_server.h  mock_define.h  mock_root_server.h  mock_server.h  mock_update_server.h
clean-local:
	-rm -f *.gcov *.gcno *.gcda
