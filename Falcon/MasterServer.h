#ifndef FALCON_MASTER_SERVER_H
#define FALCON_MASTER_SERVER_H

#include <string>
#include <boost/smart_ptr.hpp>
#include <boost/asio/io_context.hpp>
#include "HttpBase.h"
#include "ServerBase.h"

namespace falcon {

struct MasterConfig
{
	MasterConfig();

	std::string job_db_file;

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

#define MASTER_SERVER_NAME "Falcon-Master"

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
	MasterConfig config;

	boost::shared_ptr<boost::asio::io_context> client_ioctx;
	std::shared_ptr<Listener>                  client_listener;

	boost::shared_ptr<boost::asio::io_context> slave_ioctx;
	std::shared_ptr<Listener>                  slave_listener;

	std::shared_ptr<MasterHandler>             handler;
};

}

#endif
