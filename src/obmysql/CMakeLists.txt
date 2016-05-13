
SET(obmysql_sources
	ob_mysql_state.map
	ob_mysql_callback.cc
	ob_mysql_command_queue.cc
	ob_mysql_command_queue_thread.cc
	ob_mysql_dtoa.cc
	ob_mysql_field.cc
	ob_mysql_loginer.cc
	ob_mysql_result_set.cc
	ob_mysql_row.cc
	ob_mysql_server.cc
	ob_mysql_state.cc
	ob_mysql_util.cc
	packet/ob_mysql_command_packet.cc
	packet/ob_mysql_eof_packet.cc
	packet/ob_mysql_error_packet.cc
	packet/ob_mysql_field_packet.cc
	packet/ob_mysql_handshake_packet.cc
	packet/ob_mysql_ok_packet.cc
	packet/ob_mysql_packet.cc
	packet/ob_mysql_resheader_packet.cc
	packet/ob_mysql_row_packet.cc
	packet/ob_mysql_spr_packet.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "obmysql")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${obmysql_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	${EASYLIB}
	tbsys.static
	sql.static
	common.static
)
BUILD_STATIC_LIBRARY()

