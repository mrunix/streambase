AM_CPPFLAGS = \
	-I${TBLIB_ROOT}/include/tbsys \
	-I${TBLIB_ROOT}/include/tbnet \
	-I${top_srcdir}/src \
	-I${top_srcdir}/src/common
CXXFLAGS += -D__UNIT_TEST__
LDADD = \
  ${top_builddir}/src/common/libcommon.a \
	${TBLIB_ROOT}/lib/libtbnet.a \
	${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS = -lnuma -lpthread -lc -lm -lrt -ldl -laio

noinst_LIBRARIES = libobclient.a
libobclient_a_SOURCES = ob_client.cpp

EXTRA_DIST = \
	ob_client.h

