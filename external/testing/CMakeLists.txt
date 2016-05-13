########################################################################
#
# CMake file for testing library.
#
########################################################################

# Flags passed to common.
set(CFLAGS_Release "-pthread")
set(CFLAGS_Release "${CFLAGS_Release} -fno-exceptions")
set(CFLAGS_Release "${CFLAGS_Release} -fno-strict-aliasing")
set(CFLAGS_Release "${CFLAGS_Release} -Wno-unused-parameter")
set(CFLAGS_Release "${CFLAGS_Release} -Wno-missing-field-initializers")
set(CFLAGS_Release "${CFLAGS_Release} -fvisibility=hidden")
set(CFLAGS_Release "${CFLAGS_Release} -pipe")
set(CFLAGS_Release "${CFLAGS_Release} -fPIC")
set(CFLAGS_Release "${CFLAGS_Release} -Wno-format")
#set(CFLAGS_Release "${CFLAGS_Release} -Wno-unused-result")
set(CFLAGS_Release "${CFLAGS_Release} -O2")
set(CFLAGS_Release "${CFLAGS_Release} -fno-ident")
set(CFLAGS_Release "${CFLAGS_Release} -fdata-sections")
set(CFLAGS_Release "${CFLAGS_Release} -ffunction-sections")

# Flags passed to only C files.
set(CFLAGS_C_Release "")

# Flags passed to only C++ files.
set(CFLAGS_CC_Release "")

# Rules for final target.
#set(LDFLAGS_Release "-pthread")
#set(LDFLAGS_Release "${LDFLAGS_Release} -Wl,-z,noexecstack")
#set(LDFLAGS_Release "${LDFLAGS_Release} -fPIC")
#set(LDFLAGS_Release "${LDFLAGS_Release} -Wl,-threads")
#set(LDFLAGS_Release "${LDFLAGS_Release} -Wl,-thread-count=4")
#set(LDFLAGS_Release "${LDFLAGS_Release} -Wl,-icf=none")
#set(LDFLAGS_Release "${LDFLAGS_Release} -Wl,-O1")
#set(LDFLAGS_Release "${LDFLAGS_Release} -Wl,--as-needed")
#set(LDFLAGS_Release "${LDFLAGS_Release} -Wl,--gc-sections")

set(testing_cflags "${CFLAGS_Release} ${CFLAGS_C_Release} ${CFLAGS_CC_Release}")
set(testing_ldflags "${LDFLAGS_Release}")


# Include directories path.
include_directories(${PROJECT_SOURCE_DIR}/testing)
include_directories(${PROJECT_SOURCE_DIR}/testing/gtest)
include_directories(${PROJECT_SOURCE_DIR}/testing/gtest/include)

# Gtest sources.
set(gtest_srcs 
	gtest/src/gtest-death-test.cc
	gtest/src/gtest-filepath.cc
	gtest/src/gtest-port.cc
	gtest/src/gtest-printers.cc
	gtest/src/gtest-test-part.cc
	gtest/src/gtest-typed-test.cc
	gtest/src/gtest.cc
	multiprocess_func_list.cc
)
add_library(gtest ${gtest_srcs})

# Set compile flags.
set_target_properties(
	gtest
	PROPERTIES
	COMPILE_FLAGS "${testing_cflags}"
)

# Set link flags.
set_target_properties(
	gtest
	PROPERTIES
	LINK_FLAGS "${testing_ldflags}"
)

target_link_libraries(gtest stdc++)
target_link_libraries(gtest rt)
target_link_libraries(gtest pthread)
target_link_libraries(gtest dl)

# Link libraries


# GtestMain sources.
set(gtest_main_srcs 
	gtest/src/gtest_main.cc
)
add_library(gtest_main ${gtest_main_srcs})

# Set compile flags.
set_target_properties(
	gtest_main
	PROPERTIES
	COMPILE_FLAGS "${testing_cflags}"
)

# Set link flags.
set_target_properties(
	gtest_main
	PROPERTIES
	LINK_FLAGS "${testing_ldflags}"
)

target_link_libraries(gtest_main gtest)

# gmock
include_directories(${PROJECT_SOURCE_DIR}/testing/gmock)
include_directories(${PROJECT_SOURCE_DIR}/testing/gmock/include)

# Gmock sources.
set(gmock_srcs 
	gmock/src/gmock-all.cc
	gmock/src/gmock-internal-utils.cc
	gmock/src/gmock-spec-builders.cc
	gmock/src/gmock-cardinalities.cc
	gmock/src/gmock.cc
	gmock/src/gmock-matchers.cc
)
add_library(gmock ${gmock_srcs})

# Set compile flags.
set_target_properties(
	gmock
	PROPERTIES
	COMPILE_FLAGS "${testing_cflags}"
)

# Set link flags.
set_target_properties(
	gmock
	PROPERTIES
	LINK_FLAGS "${testing_ldflags}"
)

# Link libraries
target_link_libraries(gmock gtest)

# GtestMain sources.
set(gmock_main_srcs 
	gmock/src/gmock_main.cc
)
add_library(gmock_main ${gmock_main_srcs})

# Set compile flags.
set_target_properties(
	gmock_main
	PROPERTIES
	COMPILE_FLAGS "${testing_cflags}"
)

# Set link flags.
set_target_properties(
	gmock_main
	PROPERTIES
	LINK_FLAGS "${testing_ldflags}"
)

# Link libraries
target_link_libraries(gmock_main gmock)

# sample
#add_subdirectory(samples)

