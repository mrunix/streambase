AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/hash

LDADD = ${top_builddir}/src/common/libcommon.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a  

AM_LDFLAGS=-lpthread -lc -lm -lrt -lgtest   ${GCOV_LIB} -lnuma
CXXFLAGS=-D__STDC_LIMIT_MACROS -g -Wall
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_PROGRAMS = limit_array_test \
	       test_get_param \
         schema_test \
	       serialization_test \
	       test_lrucache \
	       memory_pool_test \
	       test_ob_vector \
	       wait_object_test \
	       test_ob_string_buf \
	       test_ob_object \
	       test_obscanner \
	       test_probability_random \
         test_ob_single_log_reader \
	       test_ob_log_dir_scanner \
	       test_scan_param \
	       test_ob_stat \
	       test_ob_range \
	       test_ob_log_writer \
	       test_ob_slave_mgr \
	       test_kr \
	       test_slab \
	       test_ob_string_search \
	       test_groupby_param \
	       test_ob_log_reader \
         test_file \
         test_row_compaction

test_get_param_SOURCES = test_get_param.cpp
schema_test_SOURCES = schema_test.cpp
limit_array_test_SOURCES = limit_array_test.cpp
serialization_test_SOURCES = serialization_test.cpp
memory_pool_test_SOURCES = memory_pool_test.cpp
test_lrucache_SOURCES = test_lrucache.cpp
test_ob_vector_SOURCES = test_ob_vector.cpp
wait_object_test_SOURCES = wait_object_test.cpp
test_ob_string_buf_SOURCES=test_ob_string_buf.cpp
test_ob_object_SOURCES = test_ob_object.cpp $(top_srcdir)/src/common/ob_object.cpp
test_scan_param_SOURCES=test_scan_param.cpp
test_ob_stat_SOURCES=test_ob_stat.cpp
test_ob_log_dir_scanner_SOURCES=test_ob_log_dir_scanner.cpp
test_probability_random_SOURCES=test_probability_random.cpp
test_ob_single_log_reader_SOURCES= test_ob_single_log_reader.cpp
test_ob_range_SOURCES = test_ob_range.cpp
test_ob_log_reader_SOURCES = test_ob_log_reader.cpp
test_ob_log_writer_SOURCES = test_ob_log_writer.cpp
test_ob_slave_mgr_SOURCES = test_ob_slave_mgr.cpp
test_obscanner_SOURCES = test_obscanner.cpp
test_ob_string_search_SOURCES = test_ob_string_search.cpp
test_slab_SOURCES = test_slab.cpp
test_groupby_param_SOURCES =  test_groupby_param.cpp
test_kr_SOURCES = test_kr.cpp
test_file_SOURCES = test_file.cpp
test_row_compaction_SOURCES = test_row_compaction.cpp

SUBDIRS = hash compress

DIST_SUBDIRS = hash compress

EXTRA_DIST = \
	mock_mem_iterator.h \
	test_helper.h\
	slave_mgr_4_test.h

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS) 

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda
