
SET(tbnet_lib_sources
	channel.cc
	channelpool.cc
	connection.cc
	controlpacket.cc
	defaultpacketstreamer.cc
	epollsocketevent.cc
	httppacketstreamer.cc
	httprequestpacket.cc
	httpresponsepacket.cc
	iocomponent.cc
	packet.cc
	packetqueue.cc
	packetqueuethread.cc
	serversocket.cc
	socket.cc
	socketevent.cc
	stats.cc
	tcpacceptor.cc
	tcpcomponent.cc
	tcpconnection.cc
	transport.cc
	udpcomponent.cc
	udpconnection.cc
	connectionmanager.cc
)

CLEAR_VARS()
SET(LOCAL_MODULE     "tbnet")
SET(LOCAL_CXX_FLAGS  "${TB_COMMON_FLAGS}")
SET(LOCAL_CXX_FILES  ${tbnet_lib_sources})
SET(LOCAL_LIBRARIES
	${SYSLIBS}
	tbsys.static
)
BUILD_LIBRARY()

