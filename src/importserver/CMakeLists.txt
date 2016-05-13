
ADD_CUSTOM_COMMAND(
	OUTPUT  ${PROJECT_BINARY_DIR}/bin/importcli.py
	COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/bin/importcli.py ${PROJECT_BINARY_DIR}/bin
	COMMAND cp ${CMAKE_CURRENT_SOURCE_DIR}/bin/importserver.py ${PROJECT_BINARY_DIR}/bin
)

ADD_CUSTOM_TARGET(cp_importserver_python_scripts ALL DEPENDS ${PROJECT_BINARY_DIR}/bin/importcli.py ${CMAKE_CURRENT_SOURCE_DIR}/bin/importserver.py)

