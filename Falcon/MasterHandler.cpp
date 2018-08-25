#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <json/json.h>
#include "Falcon.h"
#include "MasterServer.h"
#include "Util.h"

namespace falcon {

typedef std::map<std::string, std::string> URLParamMap;

struct Handler
{
	virtual ~Handler() { }
	virtual std::string Get(MasterServer* server, std::string target, const URLParamMap& params, http::status& status) { return std::string(); }
	virtual std::string Post(MasterServer* server, std::string target, const URLParamMap& params, const std::string& body, http::status& status) { return std::string(); }
};

struct JobsHandler : public Handler
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

		std::string job_id = Util::UUID(), err;
		// create private directory for new job
		std::string job_dir = Util::GetModulePath() + "/jobs/" + job_id;
		boost::system::error_code ec;
		if (!boost::filesystem::create_directories(job_dir, ec))
			return "Failed to create job directory: " + ec.message();
		std::ofstream ofs(job_dir + "/job_content.json", std::ios_base::out);
		ofs << body;
		ofs.close();

		// save attributes of new job into database
		time_t submit_time = time(NULL);
		SqliteDB db = server->MasterDB();
		std::ostringstream oss;
		oss << "insert into Job(id,name,type,submit_time,state) values('" << job_id << "','" << value["name"].asString() << "','"
			<< value["type"].asString() << "'," << submit_time << ",'" << ToString(Job::State::Queued) << "')";
		if (!db.Execute(oss.str(), err)) {
			boost::filesystem::remove_all(job_dir, ec);
			boost::filesystem::remove(job_dir, ec);
			return "Failed to write new job into database: " + err;
		}

		// create job object and add it to queue
		JobPtr job;
		if (FromString<Job::Type>(value["type"].asCString()) == Job::Type::Batch)
			job.reset(new BatchJob(job_id, value["name"].asString()));
		else
			job.reset(new DAGJob(job_id, value["name"].asString()));
		job->submit_time = submit_time;
		job->Assign(value);

		// notify scheduler thread by new job event
		// reply the client with new job id
		return job_id;
	}
};

struct MasterAPI
{
	Handler* FindHandler(const std::string& target)
	{
		for (auto& v : handlers) {
			if (v.first == target)
				return v.second;
		}
		return nullptr;
	}
	void RegisterHandler(const std::string& target, Handler* handler)
	{
		handlers.push_back(std::make_pair(target, handler));
	}
	std::list<std::pair<std::string, Handler*>> handlers;
};
static const int API_MAX_VERSION = 1;
static MasterAPI* APIVersionTable[API_MAX_VERSION] = { nullptr };

void MasterServer::SetupAPITable()
{
	static MasterAPI v1;
	v1.RegisterHandler("/jobs", new JobsHandler());

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
		Handler* handler = api->FindHandler(api_target);
		if (handler) {
			if (verb == http::verb::get)
				return handler->Get(this, api_target, params, status);
			else if (verb == http::verb::post)
				return handler->Post(this, api_target, params, body, status);
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
