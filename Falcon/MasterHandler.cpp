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
		Json::Value response(Json::objectValue);
		response["jobs"] = Json::Value(Json::arrayValue);
		if (params.find("id") != params.end()) {
			std::vector<std::string> job_ids;
			job_ids.push_back(params.find("id")->second);
			if (!server->State().QueryJobsJson(job_ids, response["jobs"]))
				response["error"] = "Job identified by " + job_ids[0] + " not found";
			else
				response["success"] = true;
		} else {
			bool finished = params.find("finished")->second == "true";
			int offset = std::atoi(params.find("offset")->second.c_str());
			int limits = std::atoi(params.find("limits")->second.c_str());
			if (limits <= 0) limits = std::numeric_limits<int>::max();
			server->State().QueryJobsJson(finished, offset, limits, response["jobs"]);
			response["success"] = true;
		}
		
		return response.toStyledString();
	}
	// create a new job
	virtual std::string Post(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		// convert job content to json value
		Json::Value value;
		if (!Util::ParseJsonFromString(body, value))
			return "Illegal json body for submitting new job";
		LOG(INFO) << "New job \"" << value["name"].asString() << "\" is submited from " << remote;

		std::string job_id, job_dir, err;
		// create private directory for new job
		while (true) {
			job_id  = Util::UUID();
			job_dir = Util::GetModulePath() + "/jobs/" + job_id;
			if (!boost::filesystem::exists(job_dir))
				break;
		}
		LOG(INFO) << "  Job id is allocated: " << job_id;
		boost::system::error_code ec;
		if (!boost::filesystem::create_directories(job_dir, ec))
			return "Failed to create job directory: " + ec.message();
		std::ofstream ofs(job_dir + "/job_content.json", std::ios_base::out);
		ofs << body;
		ofs.close();
		LOG(INFO) << "  Job local directory is " << job_dir;

		// save new job into database
		if (!server->State().InsertNewJob(job_id, value["name"].asString(), ToJobType(value["type"].asCString()), value, err)) {
			boost::filesystem::remove_all(job_dir, ec);
			boost::filesystem::remove(job_dir, ec);
			return err;
		}

		// notify scheduler thread by new job event
		server->NotifyScheduleEvent(ScheduleEvent::JobSubmit);

		// reply the client with new job id
		Json::Value response(Json::objectValue);
		response["status"] = "ok";
		response["job_id"] = job_id;
		return response.toStyledString();
	}
	// terminate a job
	virtual std::string Delete(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		Json::Value value;
		if (!Util::ParseJsonFromString(body, value))
			return "Illegal json body for terminating job";
		std::string job_id = value["job_id"].asString();
		LOG(INFO) << "Termination for job \"" << job_id << "\" is request from " << remote;

		Json::Value response(Json::objectValue);
		response["status"] = "ok";

		if (!server->State().SetJobSchedulable(job_id, false)) {
			response["status"] = "not found";
			return response.toStyledString();
		} else {
			TaskList tasks;
			server->State().GetExecutingTasks(tasks, job_id);
			int terminated_tasks = 0;
			for (TaskPtr& task : tasks) {
				Json::Value v(Json::objectValue);
				v["job_id"] = task->job_id;
				v["task_id"] = task->task_id;
				std::string result = HttpUtil::Delete(boost::str(boost::format("%s/tasks") % task->task_status.slave_id), v.toStyledString());
				v.clear();
				if (!Util::ParseJsonFromString(result, v))
					LOG(INFO) << "Terminate task " << task->job_id << "." << task->task_id << ": " << result;
				else {
					if (v.isMember("error"))
						LOG(INFO) << "Failed to terminate task " << task->job_id << "." << task->task_id << ": " << v["error"].asString();
					else {
						LOG(INFO) << "Terminate task " << task->job_id << "." << task->task_id << ": Success";
						terminated_tasks++;
					}
				}
			}
			response["terminated"] = terminated_tasks;
		}
		return response.toStyledString();
	}
};

// handler for "/api/v1/jobinfo" endpoint
struct JobInfoHandler : public Handler<MasterServer>
{
	// get job info
	virtual std::string Get(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, http::status& status)
	{
		std::string job_id = params.find("id")->second;
		JobPtr job = server->State().GetJob(job_id);
		if (!job)
			return "{\"error\": \"Job not found\" }";
		Json::Value response(Json::objectValue);
		response["state"] = ToString(job->job_state);
		response["progress"] = 1;
		response["submit_time"] = job->submit_time;
		response["progress"] = job->GetProgress();
		if (job->exec_time != 0)
			response["exec_time"] = job->exec_time;
		if (job->finish_time != 0)
			response["finish_time"] = job->finish_time;
		return response.toStyledString();
	}
};

// handler for "/api/v1/scheduler" endpoint
struct SchedulerHandler : public Handler<MasterServer>
{
	// run schedule cycle
	virtual std::string Get(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, http::status& status)
	{
		server->NotifyScheduleEvent(ScheduleEvent::UserRequired);
		return "{\"status\": \"ok\" }";
	}
};

// handler for "/api/v1/tasks" endpoint
struct TasksHandler : public Handler<MasterServer>
{
	virtual std::string Get(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, http::status& status)
	{
		URLParamMap::const_iterator it = params.find("job_id");
		if (it == params.end())
			return "{\"error\": \"No job id parameter specified\" }";
		Json::Value result(Json::arrayValue);
		if (server->State().QueryTasksJson((*it).second, std::vector<std::string>(), result))
			return result.toStyledString();
		else
			return "{\"error\": \"Specified job not found\" }";
	}

	virtual std::string Post(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		Json::Value value;
		if (!Util::ParseJsonFromString(body, value))
			return "Illegal json body for update tasks";
		std::string job_id = value["job_id"].asString();
		const Json::Value& ids_json = value["tasks"];
		std::vector<std::string> ids(ids_json.size());
		for (size_t i = 0; i < ids.size(); i++)
			ids[i] = ids_json[Json::ArrayIndex(i)].asString();
		Json::Value result(Json::arrayValue);
		if (server->State().QueryTasksJson(job_id, ids, result))
			return result.toStyledString();
		else
			return "{\"error\": \"Specified job not found\" }";
	}

	virtual std::string Delete(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		Json::Value value;
		if (!Util::ParseJsonFromString(body, value))
			return "Illegal json body for terminating task";
		std::string job_id = value["job_id"].asString();
		std::string task_id = value["task_id"].asString();
		LOG(INFO) << "Termination task \""<<task_id<<"\" of job \"" << job_id << "\" is request from " << remote;

		Json::Value response(Json::objectValue);

		JobPtr job = server->State().GetJob(job_id);
		if (!job) {
			response["error"] = "Job not found";
			return response.toStyledString();
		}
		TaskPtr task = job->GetTask(task_id);
		if (!task) {
			response["error"] = "task not found";
			return response.toStyledString();
		}

		Json::Value v(Json::objectValue);
		v["job_id"] = job_id;
		v["task_id"] = task_id;
		std::string result = HttpUtil::Delete(boost::str(boost::format("%s/tasks") % task->task_status.slave_id), v.toStyledString());
		if (!Util::ParseJsonFromString(result, v)) {
			LOG(INFO) << "Terminate task " << task->job_id << "." << task->task_id << ": " << result;
			response["error"] = result;
		} else {
			if (v.isMember("error")) {
				LOG(INFO) << "Failed to terminate task " << task->job_id << "." << task->task_id << ": " << v["error"].asString();
				response["error"] = v["error"].asString();
			} else {
				LOG(INFO) << "Terminate task " << task->job_id << "." << task->task_id << ": Success";
				response["status"] = "ok";
			}
		}
		return response.toStyledString();
	}
};

// handler for "/api/v1/tasks/stream" endpoint
struct TaskStreamHandler : public Handler<MasterServer>
{
	// get task stream
	virtual std::string Get(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, http::status& status)
	{
		URLParamMap::const_iterator it_jobid= params.find("job_id"), it_taskid = params.find("task_id");
		if (it_jobid == params.end() || it_taskid == params.end())
			return "{\"error\": \"No job id or task id parameter specified\" }";
		std::string job_id = it_jobid->second, task_id = it_taskid->second, type = "out";

		URLParamMap::const_iterator it = params.find("type");
		if (it != params.end())
			type = it->second;

		Json::Value response(Json::objectValue);
		response[type] = "";
		std::string log_file = boost::str(boost::format("%s/jobs/%s/%s.%s") % Util::GetModulePath() % job_id % task_id % type);
		FILE* f = fopen(log_file.c_str(), "rb");
		if (!f)
			response["error"] = "Cannot open task stream file " + log_file + ", please try later";
		else {
			int size = int(boost::filesystem::file_size(log_file));
			if (size > 0) {
				boost::scoped_array<char> buf(new char[size+1]);
				memset(buf.get(), 0, size+1);
				fread(buf.get(), 1, size, f);
				response[type] = buf.get();
			}
			fclose(f);
		}
		return response.toStyledString();
	}
};

// handler for "/api/v1/nodes" endpoint
struct NodesHandler : public Handler<MasterServer>
{
	// get job list
	virtual std::string Get(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, http::status& status)
	{
		Json::Value response(Json::objectValue);
		response["nodes"] = Json::Value(Json::arrayValue);
		if (!server->State().QueryNodesJson(response["nodes"]))
			response["error"] = "Unable to get nodes in cluster";
		else
			response["success"] = true;
		return response.toStyledString();
	}
};

struct HealthzHandler : public Handler<MasterServer>
{
	// health check
	virtual std::string Get(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, http::status& status)
	{
		Json::Value response(Json::objectValue);
		response["status"] = "ok";
		response["name"]   = server->GetName();
		return response.toStyledString();
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
		uint16_t port = value["port"].asUInt();
		LOG(INFO) << "Machine \"" << name << "\"(" << remote << ":"<<port<<") is joining cluster...";
		int cpu_count = value["cpu"]["count"].asInt();
		int cpu_freq  = value["cpu"]["frequency"].asInt();
		if (!value.isMember("resources"))
			return "No resource specified for registered machine " + remote;
		ResourceSet resources = Util::ParseResourcesJson(value["resources"]);
		std::string id = server->State().RegisterMachine(name, remote, port, value["os"].asString(), cpu_count, cpu_freq, resources);
		LOG(INFO) << "Machine \"" << name << "\" identified by \"" << id << "\" registered";

		// notify scheduler thread by new slave event
		server->NotifyScheduleEvent(ScheduleEvent::SlaveJoin);

		// tell this slave the heartbeat interval
		Json::Value response(Json::objectValue);
		response["cluster"]   = server->GetConfig().cluster_name;
		response["id"]        = id;
		response["addr"]      = remote;
		response["heartbeat"] = server->GetConfig().slave_heartbeat;
		return response.toStyledString();
	}
};

// handler for "/cluster/heartbeats" endpoint
struct HeartbeatsHandler : public Handler<MasterServer>
{
	virtual std::string Post(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		Json::Value value;
		if (!Util::ParseJsonFromString(body, value))
			return "Illegal json body for heartbeat";

		std::string slave_id = value["id"].asString();
		//DLOG(INFO) << "Heartbeat from " << slave_id << " received: Task Load=" << value["load"].asInt();
		Json::Value response(Json::objectValue);
		int finished = 0;
		if (server->State().Heartbeat(slave_id, value["updates"], finished)) {
			response["heartbeat"] = "ok";
			if (finished)
				server->NotifyScheduleEvent(ScheduleEvent::TaskFinished);
		} else
			response["heartbeat"] = "not found";
		return response.toStyledString();
	}
};

// handler for "/cluster/logs" endpoint
struct LogsHandler : public Handler<MasterServer>
{
	virtual std::string Post(MasterServer* server, const std::string& remote, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		std::string job_id = params.at("job_id"), task_id = params.at("task_id"), name = params.at("name");
		uintmax_t offset = std::atoi(params.at("offset").c_str());
		LOG(INFO) << "Save log file for task " << job_id << "." << task_id << ": Name=" << name << ", offset=" << offset;
	
		Json::Value response(Json::objectValue);
		std::string log_file = boost::str(boost::format("%s/jobs/%s/%s") % Util::GetModulePath() % job_id % name);
		FILE* f = fopen(log_file.c_str(), "ab+");
		if (!f) {
			response["error"] = boost::str(boost::format("Failed to open log file %s: %s") % log_file % strerror(errno));
			LOG(ERROR) << response["error"].asString();
			return response.toStyledString();
		}
		if (offset > 0)
			fseek(f, long(offset), SEEK_SET);
		fwrite(body.c_str(), 1, body.size(), f);
		fclose(f);
		response["status"] = "ok";
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
	v1.RegisterHandler("/jobinfo", new handler::api::JobInfoHandler());
	v1.RegisterHandler("/scheduler", new handler::api::SchedulerHandler());
	v1.RegisterHandler("/nodes", new handler::api::NodesHandler());
	v1.RegisterHandler("/tasks", new handler::api::TasksHandler());
	v1.RegisterHandler("/tasks/stream", new handler::api::TaskStreamHandler());
	v1.RegisterHandler("/healthz", new handler::api::HealthzHandler());
	master_api_table["/api/v1/"] = &v1;

	static MasterAPI cluster;
	cluster.RegisterHandler("/slaves", new handler::cluster::SlavesHandler());
	cluster.RegisterHandler("/heartbeats", new handler::cluster::HeartbeatsHandler());
	cluster.RegisterHandler("/logs", new handler::cluster::LogsHandler());
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
			else if (verb == http::verb::delete_)
				return handler->Delete(server, remote_addr, api_target, params, body, status);
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
