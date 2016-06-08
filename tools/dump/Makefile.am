AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I./include

LDADD = $(top_builddir)/src/updateserver/libupdateserver.a \
				$(top_builddir)/src/chunkserver/libchunkserver.a \
	$(top_builddir)/src/rootserver/librootserver.a \
	$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl ${GCOV_LIB} -lnuma
CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -DORDER_SERIALIZE -I${JAVA_HOME}/include -DDUMP_DEBUG

obdump_SOURCES=db_record_set.cpp oceanbase_db.cpp db_table_info.cpp db_record.cpp db_dumper.cpp \
							 db_dumper_config.cpp db_parse_log.cpp main.cpp db_dumper_writer.cpp \
							 db_dumper_mgr.cpp db_log_monitor.cpp db_thread_mgr.cpp db_msg_report.cpp \
							 db_record_filter.cpp db_dumper_checkpoint.cpp db_utils.cpp db_record_formator.cpp

bin_PROGRAMS = obdump
