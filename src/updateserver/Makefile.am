AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src
CXXFLAGS += -D__UNIT_TEST__
LDADD = $(top_builddir)/src/sstable/libsstable.a \
    ${top_builddir}/src/common/compress/libcomp.a \
    ${top_builddir}/src/common/libcommon.a \
		${top_builddir}/src/common/btree/libbtree.a \
		${top_builddir}/src/chunkserver/libchunkserver.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a 

CXXFLAGS+=-D__VERSION_ID__="\"updateserver 1.0.0.0\""
CXXFLAGS+=-D_BTREE_ENGINE_
#CXXFLAGS+=-pg
#CXXFLAGS+=-O2 -Wno-strict-aliasing
AM_LDFLAGS = -lnuma -lpthread -lc -lm -lrt -ldl -laio

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libupdateserver.a
libupdateserver_a_SOURCES = ob_hash_engine.cpp \
                            ob_trans_buffer.cpp \
                            ob_memtable.cpp \
                            ob_ups_mutator.cpp \
                            ob_schema_mgr.cpp \
                            ob_ups_table_mgr.cpp \
                            ob_ups_rpc_stub.cpp \
                            ob_update_server.cpp \
                            ob_update_server_main.cpp \
                            ob_update_server_param.cpp \
                            ob_ups_utils.cpp \
                            ob_ups_log_mgr.cpp \
                            ob_btree_engine.cpp \
                            ob_btree_engine_alloc.cpp \
                            ob_store_mgr.cpp \
                            ob_sstable_mgr.cpp \
                            ob_table_mgr.cpp \
                            ob_memtable_rowiter.cpp \
                            ob_ups_replay_runnable.cpp \
                            ob_transfer_sstable_query.cpp \
                            ob_ups_fetch_runnable.cpp \
                            ob_multi_file_utils.cpp \
                            ob_schema_mgrv2.cpp \
                            ob_commit_log_receiver.cpp \
                            ob_ups_fetch_lsync.cpp \
                            ob_ups_stat.cpp

bin_PROGRAMS = updateserver ups_mon
updateserver_SOURCES = main.cpp ${libupdateserver_a_SOURCES}
ups_mon_SOURCES = ups_mon.cpp
EXTRA_DIST = \
	ob_atomic.h \
	ob_btree_engine.h \
	ob_cellinfo_processor.h \
	ob_column_filter.h \
	ob_hash_engine.h \
	ob_memtable.h \
	ob_schema_mgr.h \
	ob_table_engine.h \
	ob_trans_buffer.h \
	ob_update_server.h \
	ob_update_server_main.h \
	ob_update_server_param.h \
	ob_ups_log_mgr.h \
	ob_ups_mutator.h \
	ob_ups_replay_runnable.h \
	ob_ups_rpc_stub.h \
	ob_ups_table_mgr.h \
	ob_ups_utils.h \
	ob_ups_stat.h \
	ob_memtank.h \
	ob_btree_engine_alloc.h \
	ups_mon.h \
	ob_ups_fetch_runnable.h \
	ob_table_mgr.h \
	ob_sstable_mgr.h \
	ob_transfer_sstable_query.h \
	ob_schema_mgrv2.h \
	ob_ups_tmps.h \
	ob_store_mgr.h \
	ob_memtable_rowiter.h \
	ob_multi_file_utils.h \
	ob_commit_log_receiver.h \
	ob_ups_fetch_lsync.h \
	ob_obi_slave_stat.h\
	updateserver.conf.template

pkgdata_data_DATA=updateserver.conf.template
pkgdata_datadir=${prefix}/etc
clean-local:
	-rm -f *.gcov *.gcno *.gcda
