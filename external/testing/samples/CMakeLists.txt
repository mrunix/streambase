########################################################################
#
# CMake file for testing library.
#
########################################################################

# Include directories path.
include_directories(${PROJECT_SOURCE_DIR}/testing/gtest/include)

# sample unittest sources.
set(sample1_unittest_srcs 
	sample1.cc
	sample1_unittest.cc
)
add_executable(sample1_unittest  ${sample1_unittest_srcs})

# Set compile flags.
set_target_properties(
	sample1_unittest
	PROPERTIES
	COMPILE_FLAGS "${testing_cflags} -fprofile-arcs -ftest-coverage"
)

# Set link flags.
set_target_properties(
	sample1_unittest
	PROPERTIES
	LINK_FLAGS "${testing_ldflags}  -fprofile-arcs -ftest-coverage"
)

# Link libraries
target_link_libraries(sample1_unittest gtest)
target_link_libraries(sample1_unittest gtest_main)

