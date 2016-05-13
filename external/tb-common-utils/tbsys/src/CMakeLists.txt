
SET(tbsys_lib_sources
	config.cc
	defaultrunnable.cc
	filequeue.cc
	filequeuethread.cc
	fileutil.cc
	process.cc
	queuethread.cc
	stringutil.cc
	tblog.cc
	tbnetutil.cc
	tbtimeutil.cc
	tbrwlock.cc
	Utility.cc
	Time.cc
	Cond.cc
	RecMutex.cc
	Shared.cc
	TbThread.cc
	StaticMutex.cc
	Mutex.cc
	Exception.cc
	ThreadException.cc
	CtrlCHandler.cc
	Timer.cc
	ThreadPool.cc
	Service.cc
	Network.cc
	profiler.cc
	bytebuffer.cc
	WarningBuffer.cc 
)

CLEAR_VARS()
SET(LOCAL_MODULE     "tbsys")
SET(LOCAL_CXX_FLAGS  "${TB_COMMON_FLAGS}")
SET(LOCAL_CXX_FILES  ${tbsys_lib_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
)
BUILD_LIBRARY()

