#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <boost/algorithm/string.hpp>
#include <json/json.h>
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
		   "groups": [
		       {
			       "name": "group1",
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

class MasterAPI
{
public:
	virtual std::string Get(std::string target, http::status& status, std::string& content_type) = 0;
	virtual std::string Post(std::string target, const std::string& body, http::status& status, std::string& content_type) = 0;
};
static const int API_MAX_VERSION = 1;
static MasterAPI* APIVersionTable[API_MAX_VERSION] = { nullptr };

class MasterAPIv1 : public MasterAPI
{
public:
	MasterAPIv1() { APIVersionTable[0] = this; }

	virtual std::string Get(std::string target, http::status& status, std::string& content_type);

	virtual std::string Post(std::string target, const std::string& body, http::status& status, std::string& content_type);
};

std::string MasterAPIv1::Get(std::string target, http::status& status, std::string& content_type)
{
	return "Hello, This is Falcon Get Response!";
}

std::string MasterAPIv1::Post(std::string target, const std::string& body, http::status& status, std::string& content_type)
{
	Json::Value value;
	Json::Reader reader;
	if (!reader.parse(body, value, false)) {
		status = http::status::bad_request;
		content_type = "text/html";
		return "Invalid json body";
	}
	return "Hello, This is Falcon Post Response!";
}

static MasterAPI* SelectAPIVersion(const std::string& target, http::status& status, std::string& content_type)
{
	static MasterAPIv1 apiv1;

	std::vector<std::string> tokens;
	boost::split(tokens, target, boost::is_any_of("/"), boost::token_compress_on);
	if (tokens.empty() || !boost::iequals(tokens[1], "api") || !boost::istarts_with(tokens[2], "v")) {
		status = http::status::bad_request;
		content_type = "text/html";
		return nullptr;
	}
	int version = std::atoi(&tokens[2][1]);
	if (version <= 0 || version > API_MAX_VERSION) {
		status = http::status::bad_request;
		content_type = "text/html";
		return nullptr;
	}
	return APIVersionTable[version - 1];
}

std::string MasterHandler::Get(std::string target, http::status& status, std::string& content_type)
{
	MasterAPI* api = SelectAPIVersion(target, status, content_type);
	if (api == nullptr)
		return "Invalid API version";
	return api->Get(target, status, content_type);
}

std::string MasterHandler::Post(std::string target, const std::string& body, http::status& status, std::string& content_type)
{
	MasterAPI* api = SelectAPIVersion(target, status, content_type);
	if (api == nullptr)
		return "Invalid API version";
	return api->Post(target, body, status, content_type);
}

}
