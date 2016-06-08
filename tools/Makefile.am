AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
        -I${top_srcdir}/tests/updateserver \
			  -I${top_srcdir}/src 

LDADD = $(top_builddir)/src/updateserver/libupdateserver.a \
	$(top_builddir)/src/rootserver/librootserver.a \
	$(top_builddir)/src/chunkserver/libchunkserver.a  \
	$(top_builddir)/src/mergeserver/libmergeserver.a \
	$(top_builddir)/src/sstable/libsstable.a \
	$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lnuma ${GCOV_LIB}
CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -DORDER_SERIALIZE -DCOMPATIBLE -D_BTREE_ENGINE_ #-O2 -fno-strict-aliasing
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = sstable_checker test_client databuilder  test_manager mergemeta gen_sstable cs_press cs_admin  merge_meta_new log_reader root_table_reader cs_info_reader gen_data_test ups_admin dumpsst gen_meta

sstable_checker_SOURCES = ob_sstable_checker.cpp
test_client_SOURCES = test_client.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
databuilder_SOURCES = ob_databuilder.cpp ob_disk_path.cpp
databuilder_CPPFLAGS= -DORDER_SERIALIZE ${AM_CPPFLAGS} #-DBUILDER_DEBUG 
test_manager_SOURCES = test_manager.cpp ob_disk_path.cpp
mergemeta_SOURCES = ob_merge_meta.cpp ob_tablet_meta.cpp
gen_sstable_SOURCES=gen_sstable.cpp ob_disk_path.cpp
gen_data_test_SOURCES=gen_data_test.cpp ob_disk_path.cpp
cs_press_SOURCES = base_client.cpp cs_press.cpp client_rpc.cpp common_func.cpp
cs_admin_SOURCES = base_client.cpp cs_admin.cpp client_rpc.cpp common_func.cpp stats.cpp ob_server_stats.cpp ob_cluster_stats.cpp ob_disk_path.cpp
dumpsst_SOURCES = dumpsst.cpp feak_disk_path.cpp common_func.cpp
gen_data_test_CPPFLAGS= -DGEN_SSTABLE_DEBUG ${AM_CPPFLAGS}
merge_meta_new_SOURCES = merge_meta_new.cpp ob_disk_path.cpp

log_reader_SOURCES = log_reader.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
root_table_reader_SOURCES = rt_root_table_reader.cpp
cs_info_reader_SOURCES = rt_cs_info_reader.cpp
ups_admin_SOURCES = ups_admin.cpp $(top_builddir)/tests/updateserver/test_utils.cpp $(top_builddir)/tests/updateserver/ob_final_data.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
gen_meta_SOURCES = gen_meta.cpp common_func.cpp ob_disk_path.cpp

EXTRA_DIST = \
	data_syntax.h \
        ob_databuilder.h \
	ob_sstable_checker.h \
	ob_tablet_meta.h \
        gen_sstable.h \
        base_client.h \
        cs_press.h \
        common_func.h \
        stats.h \
        client_rpc.h \
        ob_cluster_stats.h \
        ob_server_stats.h \
        cs_admin.h \
        gen_data_test.h \
	dumpsst.h \
        oceanbase.sh

pkgdata_data_DATA=oceanbase.sh
pkgdata_datadir=${prefix}
SUBDIRS=syschecker dump_data obsql
dist_bin_SCRIPTS=single-machine-bootstrap
clean-local:
	-rm -f *.gcov *.gcno *.gcda
