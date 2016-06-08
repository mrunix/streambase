AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I$(top_builddir)/src/common/ \
        -I${top_srcdir}/src

LDADD = $(top_builddir)/src/common/libcommon.a \
		    ${TBLIB_ROOT}/lib/libtbnet.a \
		    ${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -lnuma
CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -DORDER_SERIALIZE
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libdumpdata.a
libdumpdata_a_SOURCES = task_info.cpp \
                        task_manager.cpp \
                        task_output.cpp \
                        task_factory.cpp \
                        tablet_location.cpp \
                        rpc_stub.cpp \
                        task_server.cpp \
                        task_worker.cpp \
                        base_server.cpp \
                        base_client.cpp

noinst_PROGRAMS = task_server task_worker dump2str parse_file

dump2str_SOURCES = dump2str.cpp
parse_file_SOURCES = parse_file.cpp

task_server_SOURCES = task_server_main.cpp \
                     task_server_param.cpp \
                     task_info.cpp \
                     task_output.cpp \
                     task_manager.cpp \
                     task_factory.cpp \
                     rpc_stub.cpp \
                     tablet_location.cpp \
                     task_server.cpp \
                     base_server.cpp

task_worker_SOURCES = task_worker_main.cpp \
                     task_info.cpp \
                     rpc_stub.cpp \
                     tablet_location.cpp \
                     task_worker.cpp \
                     base_client.cpp

EXTRA_DIST = task_server.h \
						 task_worker.h \
             task_server_param.h \
             base_client.h \
             base_server.h \
             task_output.h \
             rpc_stub.h \
             task_info.h \
             task_manager.h \
             task_factory.h \
             task_packet.h \
             tablet_location.h \
             parse_file.h

DIST_SUBDIRS=test
clean-local:
	-rm -f *.gcov *.gcno *.gcda
