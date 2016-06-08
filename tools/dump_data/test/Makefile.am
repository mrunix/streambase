AM_CPPFLAGS = -I${top_srcdir}/tools/dump_data \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common/btree \
			  -I${TBLIB_ROOT}/include/tbnet \
        -I${TBLIB_ROOT}/include/tbsys

LDADD = $(top_builddir)/src/common/btree/libbtree.a \
		    $(top_builddir)/tools/dump_data/libdumpdata.a \
        $(top_builddir)/src/common/libcommon.a \
				${TBLIB_ROOT}/lib/libtbnet.a \
				${TBLIB_ROOT}/lib/libtbsys.a

CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall 
AM_LDFLAGS = -lpthread -lc -lm -lgtest -lrt -lnuma

pub_source = test_mock_root_server.cpp

bin_PROGRAMS = test_tablet_location \
              test_task_info \
              test_task_factory \
              test_task_output \
              test_task_manager


test_tablet_location_SOURCES = test_tablet_location.cpp
test_task_info_SOURCES = test_task_info.cpp
test_task_output_SOURCES = test_task_output.cpp
test_task_manager_SOURCES = test_task_manager.cpp
test_task_factory_SOURCES = test_task_factory.cpp ${pub_source}

EXTRA_DIST = test_mock_root_server.h
