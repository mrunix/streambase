
SET(rootserver_lib_sources
	ob_root_server_state.cc
	ob_daily_merge_checker.cc
	ob_heartbeat_checker.cc
	ob_rs_trigger_event_util.cc
	ob_root_inner_table_task.cc
	ob_root_async_task_queue.cc
	ob_chunk_server_manager.cc
	ob_root_sql_proxy.cc
	ob_migrate_info.cc
	ob_restart_server.cc
	ob_root_balancer.cc
	ob_root_balancer_runnable.cc
	ob_root_bootstrap.cc
	ob_root_callback.cc
	ob_root_ddl_operator.cc
	ob_root_fetch_thread.cc
	ob_root_log_manager.cc
	ob_root_log_replay.cc
	ob_root_log_worker.cc
	ob_root_meta2.cc
	ob_root_ms_provider.cc
	ob_root_rpc_stub.cc
	ob_root_server2.cc
	ob_root_stat_key.cc
	ob_root_table2.cc
	ob_root_ups_provider.cc
	ob_root_util.cc
	ob_root_worker.cc
	ob_schema_service_ms_provider.cc
	ob_schema_service_ups_provider.cc
	ob_server_balance_info.cc
	ob_tablet_info_manager.cc
	ob_ups_check_runnable.cc
	ob_ups_heartbeat_runnable.cc
	ob_ups_manager.cc
	ob_root_server_config.cc
	ob_root_monitor_table.cc
	ob_rs_after_restart_task.cc
	ob_root_operation_helper.cc
	ob_root_operation_data.cc
	ob_rs_schema_operation.cc
	ob_root_table_operation.cc
	ob_root_timer_task.cc
	ob_root_reload_config.cc
	ob_data_source_mgr.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "rootserver")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${rootserver_lib_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	roottable.static
	sql.static
	common.static
	timezone.static
)
BUILD_STATIC_LIBRARY()


SET(rootserver_sources
	${PROJECT_SOURCE_DIR}/version.cc
	ob_root_main.cc
	main.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "rootserver")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${rootserver_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	rootserver.static
)
BUILD_EXECUTABLE()


SET(schema_reader_sources
	${PROJECT_SOURCE_DIR}/version.cc
	ob_schema_reader.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "schema_reader")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${schema_reader_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	rootserver.static
)
BUILD_EXECUTABLE()


SET(checkpoint2str_sources
	${PROJECT_SOURCE_DIR}/version.cc
	ob_check_point2str.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "checkpoint2str")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${checkpoint2str_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	rootserver.static
)
BUILD_EXECUTABLE()


SET(str2checkpoint_sources
	${PROJECT_SOURCE_DIR}/version.cc
	ob_str2check_point.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "str2checkpoint")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${str2checkpoint_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	rootserver.static
)
BUILD_EXECUTABLE()


SET(rs_admin_sources
	${PROJECT_SOURCE_DIR}/version.cc
	ob_root_admin2.cc
	ob_root_admin2_main.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "rs_admin")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS} -Wno-sizeof-pointer-memaccess")
SET(LOCAL_CXX_FILES  ${rs_admin_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	rootserver.static
)
BUILD_EXECUTABLE()


SET(rs_stress_sources
	${PROJECT_SOURCE_DIR}/version.cc
	ob_rs_stress.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "rs_stress")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS} -Wno-sizeof-pointer-memaccess")
SET(LOCAL_CXX_FILES  ${rs_stress_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	rootserver.static
)
BUILD_EXECUTABLE()
