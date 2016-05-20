
set(base_main_test_src
	base_main_test.cc
	test_main.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE    "base_main_test")
SET(LOCAL_CXX_FILES ${base_main_test_src})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	common.static
)
BUILD_UNITTEST()

