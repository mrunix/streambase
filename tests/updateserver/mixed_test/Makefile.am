AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src

LDADD = $(top_builddir)/src/chunkserver/libchunkserver.a\
	  $(top_builddir)/src/mergeserver/libmergeserver.a\
    $(top_builddir)/src/updateserver/libupdateserver.a \
    $(top_builddir)/src/sstable/libsstable.a \
    $(top_builddir)/src/common/btree/libbtree.a \
    $(top_builddir)/src/common/libcommon.a \
    $(top_builddir)/src/common/compress/libcomp.a\
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS=-lpthread -lc -lm -lrt ${GCOV_LIB} -ldl -laio -lnuma
CXXFLAGS=-D__STDC_LIMIT_MACROS -g -Wall -D__UNIT_TEST__ -D_BTREE_ENGINE_
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_PROGRAMS = multi_write total_scan random_read

multi_write_SOURCES = multi_write.cpp mutator_builder.cpp rowkey_builder.cpp utils.cpp cellinfo_builder.cpp ../test_utils.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
total_scan_SOURCES = total_scan.cpp mutator_builder.cpp rowkey_builder.cpp utils.cpp cellinfo_builder.cpp ../test_utils.cpp row_checker.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp
random_read_SOURCES = random_read.cpp mutator_builder.cpp rowkey_builder.cpp utils.cpp cellinfo_builder.cpp ../test_utils.cpp row_checker.cpp $(top_builddir)/src/updateserver/ob_ups_stat.cpp

