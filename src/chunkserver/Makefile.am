AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src

LDADD = $(top_builddir)/src/sstable/libsstable.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		$(top_builddir)/src/common/libcommon.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS = -lnuma -lpthread -lc -lm -lrt -ldl -laio 
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libchunkserver.a
libchunkserver_a_SOURCES = ob_chunk_server.cpp \
						   ob_chunk_server_main.cpp \
						   ob_chunk_server_param.cpp \
						   ob_chunk_service.cpp \
						   ob_fileinfo_cache.cpp \
						   ob_tablet_manager.cpp \
						   ob_root_server_rpc.cpp \
						   ob_tablet.cpp \
						   ob_tablet_image.cpp \
						   ob_merge_reader.cpp \
						   ob_disk_manager.cpp \
						   ob_file_recycle.cpp \
						   ob_chunk_merge.cpp \
						   ob_chunk_server_merger_proxy.cpp \
						   ob_block_cache_reader.cpp \
						   ob_block_cache_loader.cpp \
						   ob_switch_cache_utility.cpp

bin_PROGRAMS = chunkserver 
chunkserver_SOURCES = ob_disk_path.cpp main.cpp ${libchunkserver_a_SOURCES}
EXTRA_DIST = \
	ob_chunk_server.h \
	ob_chunk_server_main.h \
	ob_chunk_server_merger_proxy.h \
	ob_chunk_server_param.h \
	ob_chunk_service.h \
	ob_disk_manager.h \
	ob_fileinfo_cache.h \
	ob_file_recycle.h \
	ob_merge_reader.h \
	ob_root_server_rpc.h \
	ob_tablet.h \
	ob_tablet_image.h \
	ob_tablet_manager.h \
	ob_chunk_server_stat.h \
	chunkserver.conf.template \
	ob_chunk_merge.h \
	ob_block_cache_reader.h \
	ob_block_cache_loader.h \
	ob_switch_cache_utility.h

pkgdata_data_DATA=chunkserver.conf.template
pkgdata_datadir=${prefix}/etc
clean-local:
	-rm -f *.gcov *.gcno *.gcda
