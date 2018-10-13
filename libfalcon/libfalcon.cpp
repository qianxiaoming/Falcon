#include <sstream>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#include "libfalcon.h"
#include "HttpUtil.h"

namespace falcon {

ResourceClaim::ResourceClaim()
{
	items.swap(std::map<std::string, Value>(
	{ { RESOURCE_CPU,  DEFAULT_CPU_USAGE },{ RESOURCE_FREQ,  DEFAULT_FREQ_USAGE },
	  { RESOURCE_GPU,  DEFAULT_GPU_USAGE },{ RESOURCE_MEM,  DEFAULT_MEM_USAGE },
	  { RESOURCE_DISK, DEFAULT_DISK_USAGE } }));
}

ResourceClaim::ResourceClaim(const ResourceClaim& claim) : items(claim.items)
{
}

ResourceClaim::~ResourceClaim()
{
}

std::string ResourceClaim::ToString() const
{
	std::ostringstream oss;
	ResourceClaim::const_iterator it = items.begin();
	while (it != items.end()) {
		oss << it->first << "=";
		if (it->second.type == ResourceClaim::ValueType::Int)
			oss << it->second.value.ival << ";";
		else
			oss << it->second.value.fval << ";";
		it++;
	}
	return oss.str();
}

bool ResourceClaim::Exists(const std::string& name) const
{
	return items.find(name) != items.end();
}
bool ResourceClaim::Exists(const char* name) const
{
	return items.find(name) != items.end();
}

template <typename T>
ResourceClaim& SetResourceValue(ResourceClaim& res, const std::string& name, T val)
{
	auto it = res.items.find(name);
	if (it == res.items.end())
		res.items.insert(std::make_pair(name, val));
	else {
		if (it->second.type == ResourceClaim::ValueType::Int)
			it->second.value.ival = int(val);
		else
			it->second.value.fval = float(val);
	}
	return res;
}
ResourceClaim& ResourceClaim::Set(const std::string& name, int val)
{
	return SetResourceValue(*this, name, val);
}
ResourceClaim& ResourceClaim::Set(const std::string& name, float val)
{
	return SetResourceValue(*this, name, val);
}

template <typename T>
T GetResourceValue(const ResourceClaim& res, const std::string& name, T val)
{
	auto it = res.items.find(name);
	if (it == res.items.end())
		return val;
	if (it->second.type == ResourceClaim::ValueType::Int)
		return T(it->second.value.ival);
	else
		return T(it->second.value.fval);
}

int ResourceClaim::Get(const std::string& name, int val) const
{
	return GetResourceValue(*this, name, val);
}

float ResourceClaim::Get(const std::string& name, float val) const
{
	return GetResourceValue(*this, name, val);
}

namespace api {

static bool ParseJsonFromString(const std::string& json, Json::Value& value);
static Json::Value ToJsonValue(const TaskSpec& task_spec);
static Json::Value ToJsonValue(const JobSpec& job_spec);
static Json::Value ToJsonValue(const ResourceClaim& claim);

TaskSpec::TaskSpec() : parallelism(1)
{
}

TaskSpec::TaskSpec(std::string name) : task_name(name), parallelism(1)
{
}

TaskSpec::TaskSpec(std::string name, std::string cmd, std::string cmd_args) :
	task_name(name), command(cmd), command_args(cmd_args), parallelism(1)
{
}

JobSpec::JobSpec() : priority(50)
{
}

JobSpec::JobSpec(JobType type) : job_type(type), priority(50)
{
}

JobSpec::JobSpec(JobType type, std::string name, const ResourceClaim& claims) :
	job_type(type), job_name(name), priority(50), resources(claims)
{
}

JobSpec::JobSpec(JobType type, std::string name, std::string cmd) :
	job_type(type), job_name(name), priority(50), command(cmd)
{
}

JobSpec& JobSpec::AddTask(const TaskSpec& task)
{
	tasks.push_back(task);
	return *this;
}

JobSpec& JobSpec::AddTask(std::string name, std::string cmd, std::string cmd_args)
{
	tasks.push_back(TaskSpec(name, cmd, cmd_args));
	return *this;
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

bool ComputingCluster::SubmitJob(JobSpec& job_spec, std::string* errmsg)
{
	std::string request = ToJsonValue(job_spec).toStyledString();
	std::string result = HttpUtil::Post(server_addr + "/api/v1/jobs", request);
	Json::Value response;
	if (!ParseJsonFromString(result, response)) {
		if (errmsg) *errmsg = "Invalid response: " + result;
		return false;
	}
	if (response.isMember("error")) {
		if (errmsg) *errmsg = response["error"].asString();
		return false;
	}
	job_spec.job_id = response["job_id"].asString();
	return true;
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

NodeList ComputingCluster::GetNodeList() const
{
	NodeList nodes;
	return std::move(nodes);
}

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

static Json::Value ToJsonValue(const TaskSpec& task_spec)
{
	Json::Value spec(Json::objectValue);
	spec["name"] = task_spec.task_name;
	if (!task_spec.environments.empty())
		spec["envs"] = task_spec.environments;
	if (!task_spec.labels.empty())
		spec["labels"] = task_spec.labels;
	if (!task_spec.command.empty())
		spec["exec"] = boost::replace_all_copy(task_spec.command, "\\", "/");
	if (!task_spec.work_dir.empty())
		spec["workdir"] = boost::replace_all_copy(task_spec.work_dir, "\\", "/");
	if (!task_spec.command_args.empty())
		spec["args"] = task_spec.command_args;
	if (task_spec.parallelism > 1)
		spec["parallelism"] = task_spec.parallelism;
	if (!task_spec.resources.items.empty())
		spec["resources"] = ToJsonValue(task_spec.resources);
	return spec;
}

static Json::Value ToJsonValue(const JobSpec& job_spec)
{
	Json::Value spec(Json::objectValue);
	spec["name"] = job_spec.job_name;
	spec["type"] = ToString(job_spec.job_type);
	if (!job_spec.environments.empty())
		spec["envs"] = job_spec.environments;
	if (!job_spec.labels.empty())
		spec["labels"] = job_spec.labels;
	spec["priority"] = job_spec.priority;
	if (!job_spec.command.empty())
		spec["exec"] = boost::replace_all_copy(job_spec.command, "\\", "/");
	if (!job_spec.work_dir.empty())
		spec["workdir"] = boost::replace_all_copy(job_spec.work_dir, "\\", "/");
	if (!job_spec.resources.items.empty())
		spec["resources"] = ToJsonValue(job_spec.resources);
	Json::Value tasks(Json::arrayValue);
	for (auto t : job_spec.tasks)
		tasks.append(ToJsonValue(t));
	spec["tasks"] = tasks;
	return spec;
}

static Json::Value ToJsonValue(const ResourceClaim& claim)
{
	Json::Value val(Json::objectValue);
	ResourceClaim::const_iterator it = claim.items.begin();
	while (it != claim.items.end()) {
		if (it->second.type == ResourceClaim::ValueType::Int)
			val[it->first] = it->second.value.ival;
		else
			val[it->first] = it->second.value.fval;
		it++;
	}
	return val;
}

}

const char* ToString(TaskState state)
{
	switch (state) {
	case TaskState::Queued:
		return "Queued";
	case TaskState::Dispatching:
		return "Dispatching";
	case TaskState::Executing:
		return "Executing";
	case TaskState::Completed:
		return "Completed";
	case TaskState::Failed:
		return "Failed";
	case TaskState::Aborted:
		return "Aborted";
	case TaskState::Terminated:
		return "Terminated";
	default:
		assert(false);
	}
	return "";
}

const char* ToString(JobType type)
{
	switch (type) {
	case JobType::Batch:
		return "Batch";
	case JobType::DAG:
		return "DAG";
	default:
		assert(false);
	}
	return "";
}

const char* ToString(JobState state)
{
	switch (state) {
	case JobState::Queued:
		return "Queued";
	case JobState::Waiting:
		return "Waiting";
	case JobState::Executing:
		return "Executing";
	case JobState::Halted:
		return "Halted";
	case JobState::Completed:
		return "Completed";
	case JobState::Failed:
		return "Failed";
	case JobState::Terminated:
		return "Terminated";
	default:
		assert(false);
	}
	return "";
}

JobType ToJobType(const char* type)
{
	if (strcmp(type, "Batch") == 0)
		return JobType::Batch;
	return JobType::DAG;
}

JobState ToJobState(const char* state)
{
	if (strcmp(state, "Queued") == 0)
		return JobState::Queued;
	if (strcmp(state, "Waiting") == 0)
		return JobState::Waiting;
	if (strcmp(state, "Executing") == 0)
		return JobState::Executing;
	if (strcmp(state, "Halted") == 0)
		return JobState::Halted;
	if (strcmp(state, "Completed") == 0)
		return JobState::Completed;
	if (strcmp(state, "Failed") == 0)
		return JobState::Failed;
	return JobState::Terminated;
}

TaskState ToTaskState(const char* state)
{
	if (strcmp(state, "Queued") == 0)
		return TaskState::Queued;
	if (strcmp(state, "Dispatching") == 0)
		return TaskState::Dispatching;
	if (strcmp(state, "Executing") == 0)
		return TaskState::Executing;
	if (strcmp(state, "Completed") == 0)
		return TaskState::Completed;
	if (strcmp(state, "Failed") == 0)
		return TaskState::Failed;
	if (strcmp(state, "Aborted") == 0)
		return TaskState::Aborted;
	if (strcmp(state, "Terminated") == 0)
		return TaskState::Terminated;
	return TaskState::Queued;
}

std::string ToString(const LabelList& labels)
{
	if (labels.empty())
		return "";
	std::ostringstream oss;
	LabelList::const_iterator it = labels.begin();
	oss << it->first << "=" << it->second;
	it++;
	while (it != labels.end()) {
		oss << ";" << it->first << "=" << it->second;
		it++;
	}
	return oss.str();
}

}
