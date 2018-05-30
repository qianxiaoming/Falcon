#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <boost/filesystem.hpp>
#include "Falcon.h"
#include "HttpBase.h"
#include "MasterServer.h"
#include "Util.h"

namespace falcon {

MasterConfig::MasterConfig()
	: slave_addr("0.0.0.0"),  slave_port(MASTER_SLAVE_PORT),   slave_num_threads(3),
	  client_addr("0.0.0.0"), client_port(MASTER_CLIENT_PORT), client_num_threads(2)
{
	master_db = nullptr;
}

MasterServer::MasterServer()
{
	http_handler.reset(new MasterHandler());
}

bool MasterServer::StartServer()
{
	MasterServer* server = MasterServer::Instance();
	if (!server->LoadConfiguration())
		return false;
	if (!server->RestoreHistorical())
		return false;
	if (!server->SetupSlaveHTTP())
		return false;
	if (!server->SetupClientHTTP())
		return false;
	return true;
}

void MasterServer::RunServer()
{
	MasterServer* server = MasterServer::Instance();
	server->Run();

	MasterServer::Destory();
}

int MasterServer::StopServer()
{
	MasterServer* server = MasterServer::Instance();
	return server->StopService();
}

const char* MasterServer::GetName()
{
	return FALCON_MASTER_SERVER_NAME;
}

static MasterServer* master_instance = nullptr;

MasterServer* MasterServer::Instance()
{
	if (master_instance == nullptr)
		master_instance = new MasterServer;
	return master_instance;
}

void MasterServer::Destory()
{
	delete master_instance;
	master_instance = nullptr;
}

static bool InitializeMasterDB(std::string db_file)
{
	sqlite3* db;
	int rc = sqlite3_open(db_file.c_str(), &db);
	if (rc) {
		LOG(ERROR) << "Can't open master database file: " << sqlite3_errmsg(db);
		sqlite3_close(db);
		return false;
	}
	std::vector<std::string> sqls = { "create table Job(id          text primary key asc, \
		                                                name        text, \
		                                                type        text, \
                                                        submit_time int, \
                                                        exec_time   int, \
                                                        finish_time int, \
                                                        status      text)"};
	for (std::string& sql : sqls) {
		char* errmsg = NULL;
		int rc = sqlite3_exec(db, sql.c_str(), NULL, NULL, &errmsg);
		if (rc) {
			LOG(ERROR) << "Failed to execute sql '" << sql << "<<': " << errmsg;
			sqlite3_close(db);
			boost::filesystem::remove(db_file);
			return false;
		}
	}
	
	sqlite3_close(db);
	return true;
}

bool MasterServer::LoadConfiguration()
{
	LOG(INFO) << "Loading service configuration...";
	std::string db_file = Util::GetModulePath() + "/falcon_master.db";
	if (!boost::filesystem::exists(db_file)) {
		if (!InitializeMasterDB(db_file))
			return false;
	}

	int rc = sqlite3_open(db_file.c_str(), &config.master_db);
	if (config.master_db == nullptr) {
		LOG(ERROR) << "Can not open master database file: " << db_file;
		return false;
	}
	LOG(INFO) << "Configuration loaded";
	return true;
}

bool MasterServer::RestoreHistorical()
{
	LOG(INFO) << "Restoring historical jobs...";
	int total_running_jobs = 0;
	LOG(INFO) << total_running_jobs <<" running job(s) restored";
	return true;
}

bool MasterServer::SetupSlaveHTTP()
{
	LOG(INFO) << "Setup HTTP service for slaves on port " << config.slave_addr << ":" << config.slave_port << "...";
	auto const address = boost::asio::ip::make_address(config.slave_addr);

	slave_ioctx = boost::make_shared<boost::asio::io_context>(config.slave_num_threads);
	boost::asio::io_context& ioc = *slave_ioctx;

	slave_listener = std::make_shared<Listener>(ioc, tcp::endpoint{ address, config.slave_port }, http_handler.get());
	if (!slave_listener->IsListening()) {
		slave_listener.reset();
		return false;
	}
	slave_listener->Accept();
	LOG(INFO) << "HTTP service for slaves OK";
	return true;
}

bool MasterServer::SetupClientHTTP()
{
	LOG(INFO) << "Setup HTTP service for clients on " << config.client_addr << ":" << config.client_port << "...";
	auto const address = boost::asio::ip::make_address(config.client_addr);

	client_ioctx = boost::make_shared<boost::asio::io_context>(config.client_num_threads);
	boost::asio::io_context& ioc = *client_ioctx;

	client_listener = std::make_shared<Listener>(ioc, tcp::endpoint{ address, config.client_port }, http_handler.get());
	if (!client_listener->IsListening()) {
		client_listener.reset();
		return false;
	}
	client_listener->Accept();
	LOG(INFO) << "HTTP service for clients OK";
	return true;
}

void MasterServer::Run()
{
	LOG(INFO) << "Master server is running...";
	int count = 2;
	std::mutex mutex;
	std::condition_variable cond;

	auto const worker_thread_func = [&count, &cond, &mutex](boost::asio::io_context* ioctx, int num_threads)
	{
		for (int i = num_threads - 1; i > 0; --i) {
			std::thread t([&ioctx] { ioctx->run(); });
			t.detach();
		}
		ioctx->run();

		std::unique_lock<std::mutex> lock(mutex);
		count--;
		cond.notify_all();
	};

	// run threads for clients
	std::thread client_thread(worker_thread_func, client_ioctx.get(), config.client_num_threads);
	client_thread.detach();
	
	// run threads for slaves
	std::thread slave_thread(worker_thread_func, slave_ioctx.get(), config.slave_num_threads);
	slave_thread.detach();

	// wait worker threads
	std::unique_lock <std::mutex> lock(mutex);
	cond.wait(lock, [&count] { return count == 0; });
	LOG(INFO) << "Master server is going to shutdown";
}

int MasterServer::StopService()
{
	if (client_listener)
		client_listener->Stop();
	if (slave_listener)
		slave_listener->Stop();
	if (config.master_db) {
		sqlite3_close(config.master_db);
		config.master_db = nullptr;
	}
	return EXIT_SUCCESS;
}

}
