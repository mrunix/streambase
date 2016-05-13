ADD_SUBDIRECTORY(log_tool)
ADD_SUBDIRECTORY(getquerytest)
ADD_SUBDIRECTORY(mixed_test)
ADD_SUBDIRECTORY(dump)
ADD_SUBDIRECTORY(syschecker)
ADD_SUBDIRECTORY(sstable_builder)
ADD_SUBDIRECTORY(liboblog)

SET(TOOLS_LIBRARIES
	${SYSLIBS}
	tbsys.static
	common.static
)

INCLUDE_DIRECTORIES(
	${PROJECT_SOURCE_DIR}/tests/updateserver
)

SET(TOOLS_FLAGS  "${COMMON_CXXFLAGS} -D_BTREE_ENGINE_")

SET(sstable_checker_sources
	ob_sstable_checker.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "sstable_checker")
SET(LOCAL_CXX_FLAGS  "${TOOLS_FLAGS}")
SET(LOCAL_CXX_FILES  ${sstable_checker_sources})
SET(LOCAL_LIBRARIES  ${TOOLS_LIBRARIES})
BUILD_EXECUTABLE()


SET(gen_sstable_sources
	gen_sstable.cc
	feak_disk_path.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "gen_sstable")
SET(LOCAL_CXX_FLAGS  "${TOOLS_FLAGS}")
SET(LOCAL_CXX_FILES  ${gen_sstable_sources})
SET(LOCAL_LIBRARIES  ${TOOLS_LIBRARIES} chunkserver.static)
BUILD_EXECUTABLE()


SET(gen_meta_sources
	gen_meta.cc
	common_func.cc
	feak_disk_path.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "gen_meta")
SET(LOCAL_CXX_FLAGS  "${TOOLS_FLAGS}")
SET(LOCAL_CXX_FILES  ${gen_meta_sources})
SET(LOCAL_LIBRARIES  ${TOOLS_LIBRARIES} chunkserver.static)
BUILD_EXECUTABLE()


SET(gen_data_testV3_sources
	gen_data_testV3.cc
	ob_disk_path.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "gen_data_testV3")
SET(LOCAL_CXX_FLAGS  "${TOOLS_FLAGS}")
SET(LOCAL_CXX_FILES  ${gen_data_testV3_sources})
SET(LOCAL_LIBRARIES  ${TOOLS_LIBRARIES} chunkserver.static)
BUILD_EXECUTABLE()


SET(log_reader_sources
	log_reader.cc
	${PROJECT_SOURCE_DIR}/src/updateserver/ob_ups_stat.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "log_reader")
SET(LOCAL_CXX_FLAGS  "${TOOLS_FLAGS}")
SET(LOCAL_CXX_FILES  ${log_reader_sources})
SET(LOCAL_LIBRARIES  ${TOOLS_LIBRARIES} updateserver.static)
BUILD_EXECUTABLE()


SET(cs_admin_sources
	cs_admin.cc
	client_rpc.cc
	common_func.cc
	stats.cc
	ob_server_stats.cc
	ob_cluster_stats.cc
	ob_disk_path.cc
	${PROJECT_SOURCE_DIR}/src/updateserver/ob_ups_stat.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "cs_admin")
SET(LOCAL_CXX_FLAGS  "${TOOLS_FLAGS}")
SET(LOCAL_CXX_FILES  ${cs_admin_sources})
SET(LOCAL_LIBRARIES  ${TOOLS_LIBRARIES} rootserver.static chunkserver.static)
BUILD_EXECUTABLE()


SET(dumpsst_sources
	dumpsst.cc
	feak_disk_path.cc
	common_func.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "dumpsst")
SET(LOCAL_CXX_FLAGS  "${TOOLS_FLAGS}")
SET(LOCAL_CXX_FILES  ${dumpsst_sources})
SET(LOCAL_LIBRARIES  ${TOOLS_LIBRARIES} chunkserver.static)
BUILD_EXECUTABLE()


SET(ups_admin_sources
	ups_admin.cc
	${PROJECT_SOURCE_DIR}/tests/updateserver/test_utils.cc
	${PROJECT_SOURCE_DIR}/src/updateserver/ob_ups_stat.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "ups_admin")
SET(LOCAL_CXX_FLAGS  "${TOOLS_FLAGS}")
SET(LOCAL_CXX_FILES  ${ups_admin_sources})
SET(LOCAL_LIBRARIES  ${TOOLS_LIBRARIES} updateserver.static)
BUILD_EXECUTABLE()


SET(convert_idx_file_sources
	convert_idx_file.cc
	feak_disk_path.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "convert_idx_file")
SET(LOCAL_CXX_FLAGS  "${TOOLS_FLAGS}")
SET(LOCAL_CXX_FILES  ${convert_idx_file_sources})
SET(LOCAL_LIBRARIES  ${TOOLS_LIBRARIES} chunkserver.static)
BUILD_EXECUTABLE()


SET(search_sstable_sources
	search_sstable.cc
	feak_disk_path.cc
	common_func.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "search_sstable")
SET(LOCAL_CXX_FLAGS  "${TOOLS_FLAGS}")
SET(LOCAL_CXX_FILES  ${search_sstable_sources})
SET(LOCAL_LIBRARIES  ${TOOLS_LIBRARIES} chunkserver.static)
BUILD_EXECUTABLE()


SET(oberror_sources
	oberror.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "oberror")
SET(LOCAL_CXX_FLAGS  "${TOOLS_FLAGS}")
SET(LOCAL_CXX_FILES  ${oberror_sources})
SET(LOCAL_LIBRARIES  ${TOOLS_LIBRARIES})
BUILD_EXECUTABLE()
