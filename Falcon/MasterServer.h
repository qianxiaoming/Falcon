#ifndef FALCON_MASTER_SERVER_H
#define FALCON_MASTER_SERVER_H

#include <string>
#include <boost/smart_ptr.hpp>
#include <boost/asio/io_context.hpp>
#include "HttpBase.h"
#include "ServerBase.h"
#include "sqlite3.h"

namespace falcon {

struct MasterConfig
{
	MasterConfig();

	sqlite3* master_db;

	std::string slave_addr;
	unsigned short slave_port;
	int slave_num_threads;

	std::string client_addr;
	unsigned short client_port;
	int client_num_threads;
};

class MasterHandler : public Handler
{
public:
	virtual std::string Get(std::string target, http::status& status, std::string& content_type);
	virtual std::string Post(std::string target, const std::string& body, http::status& status, std::string& content_type);
};
typedef std::shared_ptr<MasterHandler> MasterHandlerPtr;

typedef boost::shared_ptr<boost::asio::io_context> IOContextPtr;

class MasterServer : public ServerBase
{
public:
	MasterServer();

	static MasterServer* Instance();

	static void Destory();

	virtual bool StartServer();

	virtual void RunServer();

	virtual int StopServer();

	virtual const char* GetName();

public:
	bool LoadConfiguration();

	bool RestoreHistorical();

	bool SetupSlaveHTTP();

	bool SetupClientHTTP();

	void Run();

	int StopService();

private:
	MasterConfig     config;

	IOContextPtr     client_ioctx;
	ListenerPtr      client_listener;

	IOContextPtr     slave_ioctx;
	ListenerPtr      slave_listener;

	MasterHandlerPtr http_handler;

	std::mutex       queue_mutex;
	std::mutex       machine_mutex;
};

}

#endif
