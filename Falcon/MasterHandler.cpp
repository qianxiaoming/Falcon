#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <boost/algorithm/string.hpp>
#include <json/json.h>
#include "Falcon.h"
#include "MasterServer.h"
#include "Util.h"

namespace falcon {

typedef std::map<std::string, std::string> URLParamMap;

struct HandleFunc
{
	virtual ~HandleFunc() { }
	virtual std::string Get(MasterServer* server, std::string target, const URLParamMap& params, http::status& status) { return std::string(); }
	virtual std::string Post(MasterServer* server, std::string target, const URLParamMap& params, const std::string& body, http::status& status) { return std::string(); }
};

/*
1. Submit new job
POST /api/v1/jobs
Batch job:
{
	"name": "name of new job",
	"type": "Batch",
	"exec": "d:/myprogm.exe",
	"tasks": [
		{
			"name": "task1",
			"exec": "d:/myprogm.exe",
			"args": "",
			"parallel": 10
		},
		{
			"name": "task2",
			"args": "d:/test/data.txt"
		}
	]
}
DAG job:
{
	"name": "name of new job",
	"type": "dag",
	"jobs": [
		{
			"name": "group1",
			"type": "batch",
			"exec": "d:/myprogm.exe",
			"tasks": [
				{
					"name": "task1",
					"args": ""
				}
			]
		},
		{
			"name": "group2",
			"type": "batch",
			"depends": "group1",
			"exec": "d:/myprogm.exe",
			"tasks": [
				{
					"name": "task2",
					"args": ""
				}
			]
		}
	]
}
*/
struct JobsFunc : public HandleFunc
{
	// get job list
	virtual std::string Get(MasterServer* server, std::string target, const URLParamMap& params, http::status& status)
	{
		return "{ \"success\": true, \"jobs\" : [] }";
	}
	// create a new job
	virtual std::string Post(MasterServer* server, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		// convert job content to json value
		Json::Value value;
		if (!Util::ParseJsonFromString(body, value))
			return "Illegal json body for submitting new job";

		// save attributes of new job into database
		std::string job_id = Util::UUID(), err;
		SqliteDB db = server->MasterDB();
		std::ostringstream oss;
		oss << "insert into Job(id,name,type,submit_time,state) values('" << job_id << "','" << value["name"].asString() << "','"
			<< value["type"].asString() << "'," << time(NULL) << ",'" << ToString(Job::State::Queued) << "')";
		if (!db.Execute(oss.str(), err))
			return "Failed to write job infomation into database: " + err;
		// create private directory for new job
		// create job object and add it to queue
		// notify scheduler thread by new job event
		// reply the client with new job id
		return job_id;
	}
};

struct MasterAPI
{
	HandleFunc* FindHandleFunc(const std::string& target)
	{
		for (auto& v : handle_funcs) {
			if (v.first == target)
				return v.second;
		}
		return nullptr;
	}
	void RegisterHandleFunc(const std::string& target, HandleFunc* func)
	{
		handle_funcs.push_back(std::make_pair(target, func));
	}
	std::list<std::pair<std::string, HandleFunc*>> handle_funcs;
};
static const int API_MAX_VERSION = 1;
static MasterAPI* APIVersionTable[API_MAX_VERSION] = { nullptr };

void MasterServer::SetupAPITable()
{
	static MasterAPI v1;
	v1.RegisterHandleFunc("/jobs", new JobsFunc());

	APIVersionTable[0] = &v1;
}

static std::string ParseHttpURL(const std::string& target, int offset, URLParamMap& params)
{
	std::size_t pos = target.find('?');
	if (pos == std::string::npos)
		return target.substr(offset);
	std::string api_target = target.substr(offset, pos - offset);

	std::vector<std::string> p;
	boost::split(p, target.substr(pos + 1), boost::is_any_of("&"));
	for (auto& s : p) {
		pos = s.find('=');
		if (pos == std::string::npos)
			continue;
		params[s.substr(0, pos)] = s.substr(pos + 1);
	}
	return api_target;
}

std::string MasterServer::HandleClientRequest(
	http::verb verb,
	const std::string& target,
	const std::string& body,
	http::status& status)
{
	MasterAPI* api = nullptr;
	int offset = 0;
	if (boost::istarts_with(target, "/api/v1/")) {
		api = APIVersionTable[0];
		offset = sizeof("/api/v1/") - 2;
	}
	URLParamMap params;
	std::string api_target = ParseHttpURL(target, offset, params);
	if (api) {
		HandleFunc* func = api->FindHandleFunc(api_target);
		if (func) {
			if (verb == http::verb::get)
				return func->Get(this, api_target, params, status);
			else if (verb == http::verb::post)
				return func->Post(this, api_target, params, body, status);
			else {
				status = http::status::bad_request;
				return "Unsupported HTTP-method";
			}
		}
	}
	status = http::status::bad_request;
	return "Illegal request-target";
}

std::string MasterServer::HandleSlaveRequest(
	http::verb verb,
	const std::string& target,
	const std::string& body,
	http::status& status)
{
	return std::string();
}

}
