
SET(MAKE_SYS_NAME "Linux")

IF(COMPILER_PATH AND COMPILER_PREFIX)
	SET(CMAKE_C_COMPILER   ${COMPILER_PATH}/bin/${COMPILER_PREFIX}gcc)
	SET(CMAKE_CXX_COMPILER ${COMPILER_PATH}/bin/${COMPILER_PREFIX}g++)
	# where is the target environment 
	SET(CMAKE_FIND_ROOT_PATH  ${COMPILER_PATH})
ELSEIF()
	SET(CMAKE_C_COMPILER   gcc)
	SET(CMAKE_CXX_COMPILER g++)
ENDIF()

# search for programs in the build host directories
SET(CMAKE_FIND_ROOT_PATH_MODE_PROGRAM NEVER)
# for libraries and headers in the target directories
SET(CMAKE_FIND_ROOT_PATH_MODE_LIBRARY ONLY)
SET(CMAKE_FIND_ROOT_PATH_MODE_INCLUDE ONLY)


SET(NGP_C_FLAGS                  "")
SET(NGP_C_FLAGS_DEBUG            "")
SET(NGP_C_FLAGS_RELEASE          "")
SET(NGP_C_FLAGS_RELWITHDEBINFO   "")
SET(NGP_CXX_FLAGS                "")
SET(NGP_CXX_FLAGS_DEBUG          "")
SET(NGP_CXX_FLAGS_RELEASE        "")
SET(NGP_CXX_FLAGS_RELWITHDEBINFO "")
SET(NGP_SHARED_LINKER_FLAGS      "")
SET(NGP_MODULE_LINKER_FLAGS      "")
SET(NGP_EXE_LINKER_FLAGS         "")
SET(NGP_ASM_FLAGS                "-x assembler-with-cpp")
SET(NGP_COMMON_FLAGS             "-Wno-unused-local-typedefs -Wno-enum-compare -Wno-switch") # "-L. -Wall -Wno-sign-compare -Wno-unused-parameter"
SET(NGP_PROFILE_FLAGS            "-fprofile-arcs -ftest-coverage")

IF(NGP_PROFILE)
	INCLUDE(CodeCoverage)
ENDIF()

# unittest only support Linux platform currently
IF(BUILD_APP STREQUAL "unittest")
	SET(CMAKE_BUILD_TYPE Debug CACHE STRING
        "Choose the type of build, options are: None Debug Release" FORCE)

    ENABLE_TESTING()
    INCLUDE(CTest)
    INCLUDE(CodeCoverage)

	SET(NGP_C_UNITTEST_FLAGS      "-O0 -W")
    SET(NGP_CXX_UNITTEST_FLAGS    "-O0")
	SET(NGP_UNITTEST_LINKER_FLAGS "")

	ADD_DEFINITIONS(
		-D_UNITTEST_ 
	)

    MESSAGE(STATUS "Building with unittests")
ENDIF()

IF(NOT CMAKE_BUILD_TYPE)
	SET(CMAKE_BUILD_TYPE Release CACHE STRING
    	"Choose the type of build, options are: None Debug Release" FORCE)
ENDIF()

SET(NGP_ARCH_FLAGS "")
IF(TARGET_ARCH STREQUAL "ngp_arm")
    SET(NGP_ARCH_FLAGS "${NGP_ARCH_FLAGS} -mfpu=neon -funsafe-math-optimizations") # "-mfpu=vfpv3"
ENDIF()

ADD_DEFINITIONS(
	-D${TARGET_ARCH}
)

IF(TARGET_ARCH STREQUAL "ngp_ppc" OR TARGET_ARCH STREQUAL "ngp_arm" OR TARGET_ARCH STREQUAL "ngp_i386")
	SET(TARGET_IS_32_BIT "1")
ELSEIF(TARGET_ARCH STREQUAL "ngp_ppc64" OR TARGET_ARCH STREQUAL "ngp_x86_64" OR TARGET_ARCH STREQUAL "ngp_ia64")
	SET(TARGET_IS_64_BIT "1")
ENDIF()

# Library
SET(SYSLIBS stdc++ pthread rt dl nsl m)

