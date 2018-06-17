#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <boost/algorithm/string.hpp>
#include <json/json.h>
#include "Falcon.h"
#include "MasterServer.h"

namespace falcon {

/*
1. Submit new job
   POST /api/v1/jobs
   Batch job:
      {
	       "name": "name of new job",
		   "type": "batch",
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

struct HandleFunc
{
	virtual ~HandleFunc() { }
	virtual std::string Get(MasterServer* server, std::string target, http::status& status) { return std::string(); }
	virtual std::string Post(MasterServer* server, std::string target, const std::string& body, http::status& status) { return std::string(); }
};

struct JobsFunc : public HandleFunc
{
	virtual std::string Get(MasterServer* server, std::string target, http::status& status)
	{
		return "{ \"success\": true, \"jobs\" : [] }";
	}

	virtual std::string Post(MasterServer* server, std::string target, const std::string& body, http::status& status)
	{
		return std::string();
	}
};

struct MasterAPI
{
	HandleFunc* FindHandleFunc(const std::string& target)
	{
		std::list<std::pair<std::string, HandleFunc*>>::iterator begin = handle_funcs.begin(),
			end = handle_funcs.end();
		while (begin != end) {
			if (begin->first == target)
				return begin->second;
			begin++;
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

std::string MasterServer::HandleClientRequest(
	http::verb verb,
	const std::string& target,
	const std::string& body,
	http::status& status)
{
	MasterAPI* api = nullptr;
	std::string handle_target;
	if (boost::istarts_with(target, "/api/v1/")) {
		api = APIVersionTable[0];
		handle_target = target.substr(sizeof("/api/v1/") - 2);
	}
	if (api) {
		HandleFunc* func = api->FindHandleFunc(handle_target);
		if (func) {
			if (verb == http::verb::get)
				return func->Get(this, handle_target, status);
			else if (verb == http::verb::post)
				return func->Post(this, handle_target, body, status);
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
