#ifndef FALCON_MASTER_SERVER_H
#define FALCON_MASTER_SERVER_H

#include <string>
#include <boost/smart_ptr.hpp>
#include <boost/asio/io_context.hpp>
#include "HttpBase.h"
#include "ServerBase.h"
#include "Util.h"

namespace falcon {

struct MasterConfig
{
	MasterConfig();

	std::string slave_addr;
	unsigned short slave_port;
	int slave_num_threads;

	std::string client_addr;
	unsigned short client_port;
	int client_num_threads;
};

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

public:
	std::string HandleClientRequest(
		http::verb verb,
		const std::string& target,
		const std::string& body,
		http::status& status);

	std::string HandleSlaveRequest(
		http::verb verb,
		const std::string& target,
		const std::string& body,
		http::status& status);

	SqliteDB MasterDB();

private:
	void SetupAPITable();

	MasterConfig     config;

	std::mutex       db_mutex;
	sqlite3*         master_db;

	IOContextPtr     client_ioctx;
	ListenerPtr      client_listener;

	IOContextPtr     slave_ioctx;
	ListenerPtr      slave_listener;

	std::mutex       queue_mutex;
	JobList          job_queue;

	std::mutex       machine_mutex;
	MachineMap       machines;
};

}

#endif
