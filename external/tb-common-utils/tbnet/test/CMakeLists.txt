
FUNCTION(BUILD_TEST_APP name)
	SET(${name}_sources
		${name}.cc
	)

	CLEAR_VARS()
	SET(LOCAL_MODULE     "${name}")
	SET(LOCAL_CXX_FLAGS  "${TB_COMMON_FLAGS}")
	SET(LOCAL_CXX_FILES  ${${name}_sources})
	SET(LOCAL_LIBRARIES  tbnet.static tbsys.static)
	BUILD_EXECUTABLE()
ENDFUNCTION()

BUILD_TEST_APP("echoserver")
BUILD_TEST_APP("echoclient")
BUILD_TEST_APP("httpserver")

