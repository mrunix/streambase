
SET(BTREE_SOURCES
	btree_default_alloc.cc
	btree_node.cc
	btree_read_param.cc
	btree_write_handle.cc
	id_btree.cc
	key_btree.cc
	btree_base.cc
	btree_read_handle_new.cc
	btree_base_handle.cc
	btree_root_pointer.cc
	btree_array_list.cc
	btree_write_param.cc
	id_key_btree.cc
)

#-DOCEAN_BASE_BTREE_USE_SYS_MALLOC -DOCEAN_BASE_BTREE_DEBUG
CLEAR_VARS()
SET(LOCAL_MODULE     "btree")
SET(LOCAL_C_FLAGS    "${COMMON_CFLAGS}")
SET(LOCAL_CXX_FLAGS  "${COMMON_CXXFLAGS}")
SET(LOCAL_CXX_FILES  ${BTREE_SOURCES})
SET(LOCAL_LIBRARIES  ${SYSLIBS})
BUILD_STATIC_LIBRARY()

