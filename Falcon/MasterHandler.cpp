#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <json/json.h>
#include "Falcon.h"
#include "MasterServer.h"
#include "Util.h"

namespace falcon {

namespace handler {
namespace api {

// handler for "/api/v1/jobs" endpoint
struct JobsHandler : public Handler<MasterServer>
{
	// get job list
	virtual std::string Get(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, http::status& status)
	{
		return "{ \"success\": true, \"jobs\" : [] }";
	}
	// create a new job
	virtual std::string Post(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		// convert job content to json value
		Json::Value value;
		if (!Util::ParseJsonFromString(body, value))
			return "Illegal json body for submitting new job";

		std::string job_id, job_dir, err;
		// create private directory for new job
		while (true) {
			job_id  = Util::UUID();
			job_dir = Util::GetModulePath() + "/jobs/" + job_id;
			if (!boost::filesystem::exists(job_dir))
				break;
		}
		boost::system::error_code ec;
		if (!boost::filesystem::create_directories(job_dir, ec))
			return "Failed to create job directory: " + ec.message();
		std::ofstream ofs(job_dir + "/job_content.json", std::ios_base::out);
		ofs << body;
		ofs.close();

		// save new job into database
		if (!server->State().InsertNewJob(job_id, value["name"].asString(), FromString<Job::Type>(value["type"].asCString()), value, err)) {
			boost::filesystem::remove_all(job_dir, ec);
			boost::filesystem::remove(job_dir, ec);
			return err;
		}

		// notify scheduler thread by new job event
		server->NotifyScheduleEvent(ScheduleEvent::JobSubmit);

		// reply the client with new job id
		return job_id;
	}
};

}

namespace cluster {

// handler for "/cluster/slaves" endpoint
struct SlavesHandler : public Handler<MasterServer>
{
	// register new slave
	virtual std::string Post(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		Json::Value value;
		if (!Util::ParseJsonFromString(body, value))
			return "Illegal json body for registering slave";

		// register this slave in data state
		std::string name = value["name"].asString();
		LOG(INFO) << "Machine '" << name << "'(" << remote << ") is joining cluster...";
		int cpu_count = value["cpu"]["count"].asInt();
		int cpu_freq  = value["cpu"]["frequency"].asInt();
		if (!value.isMember("resources"))
			return "No resource specified for registered machine " + remote;
		ResourceSet resources = Util::ParseResourcesJson(value["resources"]);
		server->State().RegisterMachine(name, remote, value["os"].asString(), cpu_count, cpu_freq, resources);
		LOG(INFO) << "Machine '" << name << "' registered";

		// notify scheduler thread by new slave event
		server->NotifyScheduleEvent(ScheduleEvent::SlaveJoin);

		// tell this slave the heartbeat interval
		Json::Value response(Json::objectValue);
		response["cluster"]   = server->GetConfig().cluster_name;
		response["heartbeat"] = server->GetConfig().slave_heartbeat;
		return response.toStyledString();
	}
};

}
}

struct MasterAPI
{
	Handler<MasterServer>* FindHandler(const std::string& target)
	{
		for (auto& v : handlers) {
			if (v.first == target)
				return v.second;
		}
		return nullptr;
	}
	void RegisterHandler(const std::string& target, Handler<MasterServer>* handler)
	{
		handlers.push_back(std::make_pair(target, handler));
	}
	std::list<std::pair<std::string, Handler<MasterServer>*>> handlers;
};
typedef std::map<std::string, MasterAPI*> MasterAPITable;
static MasterAPITable master_api_table;

void MasterServer::SetupAPIHandler()
{
	static MasterAPI v1;
	v1.RegisterHandler("/jobs", new handler::api::JobsHandler());
	master_api_table["/api/v1/"] = &v1;

	static MasterAPI cluster;
	cluster.RegisterHandler("/slaves", new handler::cluster::SlavesHandler());
	master_api_table["/cluster/"] = &cluster;
}

static std::string HandleHttpRequest(
	MasterServer* server,
	const std::string& remote_addr,
	std::string prefix,
	http::verb verb,
	const std::string& target,
	const std::string& body,
	http::status& status)
{
	MasterAPI* api = nullptr;
	MasterAPITable::iterator it = master_api_table.find(prefix);
	if (it == master_api_table.end()) {
		status = http::status::bad_request;
		return "Illegal request-target";
	}
	api = it->second;
	
	URLParamMap params;
	std::string api_target = HttpUtil::ParseHttpURL(target, prefix.length() - 1, params);
	if (api) {
		Handler<MasterServer>* handler = api->FindHandler(api_target);
		if (handler) {
			if (verb == http::verb::get)
				return handler->Get(server, remote_addr, api_target, params, status);
			else if (verb == http::verb::post)
				return handler->Post(server, remote_addr, api_target, params, body, status);
			else {
				status = http::status::bad_request;
				return "Unsupported HTTP-method";
			}
		}
	}
	status = http::status::bad_request;
	return "Illegal request-target";
}

std::string MasterServer::HandleClientRequest(
	const std::string& remote_addr,
	http::verb verb,
	const std::string& target,
	const std::string& body,
	http::status& status)
{
	return HandleHttpRequest(this, remote_addr, "/api/v1/", verb, target, body, status);
}

std::string MasterServer::HandleSlaveRequest(
	const std::string& remote_addr,
	http::verb verb,
	const std::string& target,
	const std::string& body,
	http::status& status)
{
	return HandleHttpRequest(this, remote_addr, "/cluster/", verb, target, body, status);
}

}
