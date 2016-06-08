AM_CPPFLAGS = -I${top_srcdir}/src/common/compress

LDADD = $(top_builddir)/src/common/compress/libcomp.a 

AM_LDFLAGS=-lpthread -lc -lm -lgtest -ldl ${GCOV_LIB}
CXXFLAGS=-D__STDC_LIMIT_MACROS -g -Wall #-fprofile-arcs -ftest-coverage
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif
bin_PROGRAMS=test_libcomp

test_libcomp_SOURCES = test_libcomp.cpp

