#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <boost/filesystem.hpp>
#include <boost/bind.hpp>
#include "Falcon.h"
#include "HttpBase.h"
#include "SlaveServer.h"

namespace falcon {

SlaveServer::SlaveServer() 
	: cpu_count(0), cpu_frequency(0), slave_addr("0.0.0.0"), slave_port(SLAVE_LISTEN_PORT), registered(false),
	  hb_interval(5), hb_elapsed(0), hb_counter(0)
{
}

bool SlaveServer::StartServer()
{
	// get machine information
	if (!CollectSystemInfo())
		return false;

	SlaveServer* server = SlaveServer::Instance();
	server->SetupAPIHandler();
	if (!server->SetupListenHTTP())
		return false;

	return true;
}

bool SlaveServer::RegisterSlave()
{
	LOG(INFO) << "Registering in master server " << master_addr << "...";
	registered = false;
	Json::Value reg_info(Json::objectValue), res_info(Json::objectValue);
	reg_info["name"]      = slave_name;
	reg_info["os"]        = os_name;
	reg_info["version"]   = os_version;

	Json::Value cpu_info(Json::objectValue);
	cpu_info["count"]     = cpu_count;
	cpu_info["frequency"] = cpu_frequency;
	reg_info["cpu"]       = cpu_info;

	reg_info["resources"] = slave_resources.ToJson();

	int max_try_count = 10, try_count = 0;
	while (!IsStopped()) {
		std::string request = reg_info.toStyledString();
		std::string result = HttpUtil::Post(master_addr + "/cluster/slaves", request);
		if (result.empty()) {
			LOG(ERROR) << "Failed to register salve server! Try again later.";
			std::this_thread::sleep_for(std::chrono::seconds((int)std::pow(2, try_count)));
			try_count++;
			if (try_count > max_try_count)
				break;
		} else {
			Json::Value response;
			if (!Util::ParseJsonFromString(result, response)) {
				LOG(ERROR) << "Invalid response from master server: " << result;
				return false;
			}
			if (response.isMember("error")) {
				LOG(ERROR) << "Failed to register in master server: " << response["error"].asString();
				return false;
			}
			cluster_name = response["cluster"].asString();
			hb_interval  = response["heartbeat"].asInt();
			registered   = true;
			LOG(INFO) << "Server registered in cluster " << cluster_name << " and heartbeat interval is " << hb_interval << " seconds";
			return true;
		}
	}
	return false;
}

void SlaveServer::RunServer()
{
	SlaveServer* server = SlaveServer::Instance();
	LOG(INFO) << "Slave server is running...";

	// register in master server
	if (!RegisterSlave())
		return;

	// task update thread


	// run event loop
	hb_timer.reset(new boost::asio::steady_timer(*ioctx, boost::asio::chrono::seconds(1)));
	hb_timer->async_wait(boost::bind(&SlaveServer::Heartbeat, this, _1));
	ioctx->run();
	LOG(INFO) << "Slave server is going to shutdown";

	SlaveServer::Destory();
}

int SlaveServer::StopServer()
{
	is_stopped.store(true);
	if (listener)
		listener->Stop();
	return EXIT_SUCCESS;
}

const char* SlaveServer::GetName()
{
	return FALCON_SLAVE_SERVER_NAME;
}

static SlaveServer* slave_instance = nullptr;

SlaveServer* SlaveServer::Instance()
{
	if (slave_instance == nullptr)
		slave_instance = new SlaveServer;
	return slave_instance;
}

void SlaveServer::Destory()
{
	delete slave_instance;
	slave_instance = nullptr;
}

void SlaveServer::SetMasterAddr(std::string addr)
{
	master_addr = addr;
	if (master_addr.find(':') == std::string::npos)
		master_addr += boost::str(boost::format(":%d") % MASTER_SLAVE_PORT);
}

bool SlaveServer::CollectSystemInfo()
{
	char name[256] = { 0 };
	if (gethostname(name, sizeof(name)) != 0) {
		PLOG(ERROR) << "Unable to get host name";
		return false;
	}
	slave_name = name;
	os_name = "Windows"; // just for now
	os_version = "";

	std::string proc_name;
	Util::GetCPUInfo(proc_name, cpu_count, cpu_frequency);
	if (cpu_count == 0 || cpu_frequency == 0)
		return false;
	slave_resources.Set(RESOURCE_CPU, float(cpu_count));
	slave_resources.Set(RESOURCE_FREQ, cpu_count*cpu_frequency);
	slave_resources.Set(RESOURCE_MEM, Util::GetTotalMemory());

	std::string gpu_name;
	int num_gpus = 0, gpu_cores = 0;
	Util::GetGPUInfo(gpu_name, num_gpus, gpu_cores);
	slave_resources.Set(RESOURCE_GPU, num_gpus);
	// TODO Get the disk spaces
	slave_resources.Set(RESOURCE_DISK, 2048);
	return true;
}

bool SlaveServer::SetupListenHTTP()
{
	LOG(INFO) << "Setup HTTP service for cluster master on " << slave_addr << ":" << slave_port << "...";
	auto const address = boost::asio::ip::make_address(slave_addr);

	ioctx = boost::make_shared<boost::asio::io_context>();
	boost::asio::io_context& ioc = *ioctx;

	listener = std::make_shared<Listener>(
		ioc,
		tcp::endpoint{ address, slave_port },
		boost::bind(&SlaveServer::HandleMasterRequest, this, _1, _2, _3, _4, _5));
	if (!listener->IsListening()) {
		listener.reset();
		return false;
	}
	listener->Accept();
	LOG(INFO) << "HTTP service for cluster master OK";
	return true;
}

struct SlaveAPI
{
	Handler<SlaveServer>* FindHandler(const std::string& target)
	{
		for (auto& v : handlers) {
			if (v.first == target)
				return v.second;
		}
		return nullptr;
	}
	void RegisterHandler(const std::string& target, Handler<SlaveServer>* handler)
	{
		handlers.push_back(std::make_pair(target, handler));
	}
	std::list<std::pair<std::string, Handler<SlaveServer>*>> handlers;
};
static SlaveAPI slave_api;

std::string SlaveServer::HandleMasterRequest(
	const std::string& remote_addr,
	http::verb verb,
	const std::string& target,
	const std::string& body,
	http::status& status)
{
	URLParamMap params;
	std::string api_target = HttpUtil::ParseHttpURL(target, 0, params);

	Handler<SlaveServer>* handler = slave_api.FindHandler(api_target);
	if (handler) {
		if (verb == http::verb::get)
			return handler->Get(this, remote_addr, api_target, params, status);
		else if (verb == http::verb::post)
			return handler->Post(this, remote_addr, api_target, params, body, status);
		else {
			status = http::status::bad_request;
			return "Unsupported HTTP-method";
		}
	}

	status = http::status::bad_request;
	return "Illegal request-target";
}

// send heartbeat or task update information to master server
void SlaveServer::Heartbeat(const boost::system::error_code&)
{
	hb_counter++;
	// check for task update information and send as heartbeat if available

	// check if heartbeat must be send
	if (hb_elapsed < hb_interval)
		hb_elapsed++;  // do nothing, just increase elapsed counter
	else {
		// must send heartbeat information

		hb_elapsed = 0;
	}
	if (!IsStopped())
		hb_timer->async_wait(boost::bind(&SlaveServer::Heartbeat, this, _1));
}

namespace handler {
namespace slave {

// handler for "/tasks" endpoint
struct TasksHandler : public Handler<SlaveServer>
{
	// get new task from master
	virtual std::string Post(SlaveServer* server, const std::string& remote, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		Json::Value value;
		if (!Util::ParseJsonFromString(body, value))
			return "Illegal json body for registering slave";

		//std::string name = value["name"].asString();
		//LOG(INFO) << "Machine '" << name << "'(" << remote << ") is joining cluster...";
		//if (!value.isMember("resources"))
		//	return "No resource specified for registered machine " + remote;
		//ResourceMap resources = Util::ParseResourcesJson(value["resource"]);
		//server->GetDataState().RegisterMachine(name, remote, value["os"].asString(), resources);

		//// notify scheduler thread by new slave event
		//server->NotifyScheduleEvent(ScheduleEvent::SlaveJoin);
		//LOG(INFO) << "Machine '" << name << "' registered";

		Json::Value response(Json::objectValue);
		response["status"] = ToString(Task::State::Executing);
		return response.toStyledString();
	}
};

}
}

void SlaveServer::SetupAPIHandler()
{
	slave_api.RegisterHandler("/tasks", new handler::slave::TasksHandler());
}

}
