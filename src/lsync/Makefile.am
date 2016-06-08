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
CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -DORDER_SERIALIZE -DCOMPATIBLE -D__LSYNC_DEBUG__ #-O2 -fno-strict-aliasing
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = lsyncserver
lsyncserver_SOURCES = main.cpp  ob_lsync_server_main.cpp  ob_lsync_server_param.cpp  ob_lsync_server.cpp  ob_seekable_log_reader.cpp
nodist_lsyncserver_SOURCES = ${top_srcdir}/svn_version.cpp
noinst_LIBRARIES = liblsyncserver.a
liblsyncserver_a_SOURCES = ob_lsync_server_param.cpp  ob_lsync_server.cpp  ob_seekable_log_reader.cpp

pkgdata_data_DATA=lsyncserver.conf.template
pkgdata_datadir=${prefix}/etc
EXTRA_DIST=ob_lsync_server.h  ob_lsync_server_main.h  ob_lsync_server_param.h  ob_seekable_log_reader.h \
           lsyncserver.conf.template

clean-local:
	-rm -f *.gcov *.gcno *.gcda
