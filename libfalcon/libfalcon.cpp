#include <sstream>
#include <boost/format.hpp>
#include "libfalcon.h"
#include "HttpUtil.h"

namespace falcon {
namespace api {

static bool ParseJsonFromString(const std::string& json, Json::Value& value)
{
	std::istringstream iss(json);
	Json::CharReaderBuilder builder;
	builder["collectComments"] = false;
	std::string errs;
	if (!Json::parseFromStream(builder, iss, &value, &errs))
		return false;
	return true;
}

ComputingCluster::ComputingCluster(const std::string& server, uint16_t port)
	: server_addr(server), connected(false)
{
	server_addr = boost::str(boost::format("%s:%d") % server % port);
	std::string result = HttpUtil::Get(server_addr + "/api/v1/healthz");
	Json::Value value;
	if (ParseJsonFromString(result, value) && value.isMember("status")) {
		connected = value["status"].asString() == "ok";
		if (connected)
			cluster_name = value["name"].asString();
	}
}

bool ComputingCluster::IsConnected() const
{
	return connected;
}

std::string ComputingCluster::GetName() const
{
	return cluster_name;
}

std::string ComputingCluster::SubmitJob()
{
	return "";
}

bool ComputingCluster::TerminateJob(const std::string& id)
{
	return true;
}

JobPtr ComputingCluster::QueryJob(const std::string& id) const
{
	return JobPtr();
}

JobList ComputingCluster::QueryJobList() const
{
	return JobList();
}

}
}
