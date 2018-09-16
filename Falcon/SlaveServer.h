#ifndef FALCON_SLAVE_SERVER_H
#define FALCON_SLAVE_SERVER_H

#include <string>
#include <boost/smart_ptr.hpp>
#include <boost/asio/io_context.hpp>
#include "blockingconcurrentqueue.h"
#include "HttpBase.h"
#include "ServerBase.h"
#include "Util.h"

namespace falcon {

typedef boost::scoped_ptr<boost::asio::steady_timer> HeartbeatTimerPtr;

class SlaveServer : public ServerBase
{
public:
	SlaveServer();

	static SlaveServer* Instance();

	static void Destory();

	virtual bool StartServer();

	virtual void RunServer();

	virtual int StopServer();

	virtual const char* GetName();

public:
	void SetMasterAddr(std::string addr);

	bool SetupListenHTTP();

	std::string HandleMasterRequest(
		const std::string& remote_addr,
		http::verb verb,
		const std::string& target,
		const std::string& body,
		http::status& status);

private:
	void SetupAPIHandler();

	bool CollectSystemInfo();

	bool RegisterSlave();

	void Heartbeat(const boost::system::error_code&);

	std::string               cluster_name;
	std::string               slave_name;
	std::string               os_name;
	std::string               os_version;
	int                       cpu_count;
	int                       cpu_frequency;
	ResourceSet               slave_resources;

	std::string               slave_addr;
	unsigned short            slave_port;
	std::string               master_addr;
	bool                      registered;

	int                       hb_interval;
	int                       hb_elapsed;
	int                       hb_counter;
	HeartbeatTimerPtr         hb_timer;

	IOContextPtr              ioctx;
	ListenerPtr               listener;
};

}

#endif
