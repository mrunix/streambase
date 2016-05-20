
FUNCTION(BUILD_TEST_APP name)
	SET(${name}_sources
		${name}.cc
		${ARGN}
	)

	CLEAR_VARS()
	SET(LOCAL_MODULE     "${name}")
	SET(LOCAL_CXX_FLAGS  "${TB_COMMON_FLAGS}")
	SET(LOCAL_CXX_FILES  ${${name}_sources})
	SET(LOCAL_LIBRARIES  tbsys.static)
	BUILD_EXECUTABLE()
ENDFUNCTION()

BUILD_TEST_APP("testfilequeue")
BUILD_TEST_APP("testqueuethread")
BUILD_TEST_APP("testconfig")
BUILD_TEST_APP("teststringutil")
BUILD_TEST_APP("testnetutil")
BUILD_TEST_APP("testlog")
BUILD_TEST_APP("testfileutil")
BUILD_TEST_APP("testtimeutil")
BUILD_TEST_APP("testthread" testBase.cc)
BUILD_TEST_APP("testtimer" testBase.cc)
BUILD_TEST_APP("testthreadpool" testBase.cc)
BUILD_TEST_APP("testService" testBase.cc)
BUILD_TEST_APP("testwarningbuffer")

