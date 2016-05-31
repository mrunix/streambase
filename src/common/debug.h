#ifdef __rs_debug__
sb::rootserver::ObRootServer2* __rs = NULL;
sb::common::ObRoleMgr* __role_mgr = NULL;
sb::common::ObiRole* __obi_role = NULL;
#define __debug_init__() __rs = &root_server_; __role_mgr = &role_mgr_;  __obi_role = (typeof(__obi_role))&root_server_.get_obi_role();
#endif

#ifdef __ns_debug__
sb::nameserver::NameServer* __ns = NULL;
sb::common::ObRoleMgr* __role_mgr = NULL;
sb::common::ObiRole* __obi_role = NULL;
#define __debug_init__() __rs = &name_server_; __role_mgr = &role_mgr_;  __obi_role = (typeof(__obi_role))&name_server_.get_obi_role();
#endif

#ifdef __ms_debug__
sb::mergeserver::ObMergeServer* __ms = NULL;
sb::common::ObMergerSchemaManager* __ms_schema = NULL;
sb::mergeserver::ObMergeServerService* __ms_service = NULL;
#define __debug_init__() __ms = merge_server_; __ms_schema = schema_mgr_; __ms_service =  this;
#endif

#ifdef __ups_debug__
sb::updateserver::ObUpdateServer* __ups = NULL;
sb::updateserver::ObUpsRoleMgr* __role_mgr = NULL;
sb::updateserver::ObUpsLogMgr* __log_mgr = NULL;
sb::common::ObiRole* __obi_role = NULL;
#define __debug_init__() __ups = this; __role_mgr = &role_mgr_; __obi_role = &obi_role_; __log_mgr = &log_mgr_;
#endif
