#include <sstream>
#include <boost/format.hpp>
#include <boost/algorithm/string.hpp>
#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include "libfalcon.h"
#include "HttpUtil.h"

namespace falcon {

ResourceClaim::ResourceClaim()
{
}

ResourceClaim::ResourceClaim(const ResourceClaim& claim) : items(claim.items)
{
}

ResourceClaim::~ResourceClaim()
{
}

bool ResourceClaim::IsEmpty() const
{
	return items.empty();
}

void ResourceClaim::UseDefault()
{
	items.swap(std::map<std::string, Value>(
	{ { RESOURCE_CPU,  DEFAULT_CPU_USAGE },{ RESOURCE_FREQ,  DEFAULT_FREQ_USAGE },
	  { RESOURCE_GPU,  DEFAULT_GPU_USAGE },{ RESOURCE_MEM,  DEFAULT_MEM_USAGE },
	  { RESOURCE_DISK, DEFAULT_DISK_USAGE } }));
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
static void FromJsonValue(JobSpec& spec, const Json::Value& value);
static void FromJsonValue(TaskSpec& spec, const Json::Value& value);
static void FromJsonValue(ResourceClaim& claim, const Json::Value& value);
static void FromJsonValue(TaskInfo& info, const Json::Value& value);
static void FromJsonValue(NodeInfo& info, const Json::Value& value);

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

TaskInfo::TaskInfo() 
	: task_state(TaskState::Queued), progress(0), start_time(0), finish_time(0), exit_code(0)
{
}

bool TaskInfo::IsFinished() const
{
	return task_state == TaskState::Aborted || task_state == TaskState::Completed ||
		task_state == TaskState::Failed || task_state == TaskState::Terminated;
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
	if (task.resources.IsEmpty()) {
		TaskSpec spec = task;
		spec.resources = resources;
		tasks.push_back(spec);
	} else 
		tasks.push_back(task);
	return *this;
}

JobSpec& JobSpec::AddTask(std::string name, std::string cmd, std::string cmd_args)
{
	TaskSpec spec(name, cmd, cmd_args);
	spec.resources = resources;
	tasks.push_back(spec);
	return *this;
}

JobInfo::JobInfo()
	: job_state(JobState::Queued), progress(0), submit_time(0), exec_time(0), finish_time(0)
{
}

NodeInfo::NodeInfo() : state(MachineState::Unknown), online(0)
{
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

std::string ComputingCluster::GetServerAddr() const
{
	return server_addr;
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
	std::string request = boost::str(boost::format("{ \"job_id\": \"%s\" }") % id);
	std::string result = HttpUtil::Delete(server_addr + "/api/v1/jobs", request);
	Json::Value response;
	if (!ParseJsonFromString(result, response))
		return false;
	if (response.isMember("error")) {
		LOG(ERROR) << "Failed to terminate job " << id << ": " << response["error"].asString();
		return false;
	}
	return response["status"].asString() == "ok";
}

JobPtr ComputingCluster::QueryJob(const std::string& id) const
{
	JobPtr job;
	std::string result = HttpUtil::Get(server_addr + "/api/v1/jobs?id=" + id);
	Json::Value response;
	if (!ParseJsonFromString(result, response))
		return job;
	if (response.isMember("error")) {
		LOG(ERROR) << "Unable to get job identified by " << id << ": " << response["error"].asString();
		return job;
	}
	const Json::Value& jobs_json = response["jobs"];
	if (jobs_json.size() == 0)
		return job;
	job.reset(new Job(const_cast<ComputingCluster*>(this), id));
	FromJsonValue(job->GetSpec(), jobs_json[0]);
	return job;
}

JobList ComputingCluster::QueryJobList() const
{
	std::string result = HttpUtil::Get(server_addr + "/api/v1/jobs");
	Json::Value response;
	if (!ParseJsonFromString(result, response))
		return JobList();
	if (response.isMember("error")) {
		LOG(ERROR) << "Unable to get job list: " << response["error"].asString();
		return JobList();
	}

	JobList jobs;
	const Json::Value& jobs_json = response["jobs"];
	for (Json::ArrayIndex i = 0; i < jobs_json.size(); i++) {
		const Json::Value& job_json = jobs_json[i];
		JobPtr job(new Job(const_cast<ComputingCluster*>(this), job_json["id"].asString()));
		FromJsonValue(job->GetSpec(), job_json);
		jobs.push_back(job);
	}
	return std::move(jobs);
}

NodeList ComputingCluster::GetNodeList() const
{
	NodeList nodes;
	std::string result = HttpUtil::Get(GetServerAddr() + "/api/v1/nodes");
	Json::Value response;
	if (!ParseJsonFromString(result, response))
		return nodes;
	if (response.isMember("error")) {
		LOG(ERROR) << "Unable to get nodes in this cluster: " << response["error"].asString();
		return nodes;
	}
	Json::Value& nodes_json = response["nodes"];
	for (Json::ArrayIndex i = 0; i < nodes_json.size(); i++) {
		NodeInfo info;
		FromJsonValue(info, nodes_json[i]);
		nodes.push_back(info);
	}
	return std::move(nodes);
}

Job::Job(ComputingCluster* c, std::string id) : cluster(c)
{
	job_spec.job_id = id;
}

bool Job::UpdateJobInfo(JobInfo& info)
{
	std::string result = HttpUtil::Get(cluster->GetServerAddr() + "/api/v1/jobinfo?id=" + job_spec.job_id);
	Json::Value response;
	if (!ParseJsonFromString(result, response))
		return false;
	if (response.isMember("error")) {
		LOG(ERROR) << "Unable to get job info identified by " << job_spec.job_id << ": " << response["error"].asString();
		return false;
	}
	info.job_state   = ToJobState(response["state"].asCString());
	info.progress    = response["progress"].asInt();
	info.submit_time = response.isMember("submit_time") ? response["submit_time"].asInt64() : 0;
	info.exec_time   = response.isMember("exec_time") ? response["exec_time"].asInt64() : 0;
	info.finish_time = response.isMember("finish_time") ? response["finish_time"].asInt64() : 0;
	return true;
}

TaskInfoList Job::GetTaskList()
{
	TaskInfoList tasks;
	std::string result = HttpUtil::Get(cluster->GetServerAddr() + "/api/v1/tasks?job_id=" + job_spec.job_id);
	Json::Value response;
	if (!ParseJsonFromString(result, response))
		return tasks;
	if (response.type() == Json::objectValue) {
		LOG(ERROR) << "Unable to get task list for job " << job_spec.job_id << ": " << response["error"].asString();
		return tasks;
	}
	for (Json::ArrayIndex i = 0; i < response.size(); i++) {
		TaskInfo info;
		FromJsonValue(info, response[i]);
		tasks.push_back(info);
	}
	return std::move(tasks);
}

bool Job::UpdateTaskInfo(TaskInfoList& tasks)
{
	if (tasks.empty())
		return true;
	TaskInfoList::iterator removed = std::remove_if(tasks.begin(), tasks.end(), [&tasks](const TaskInfo& info) {
		return info.task_state == TaskState::Aborted || info.task_state == TaskState::Completed ||
			   info.task_state == TaskState::Failed  || info.task_state == TaskState::Terminated;
	});
	if (removed != tasks.end()) {
		tasks.erase(removed, tasks.end());
		if (tasks.empty())
			return true;
	}

	Json::Value request(Json::objectValue);
	request["job_id"] = job_spec.job_id;
	request["tasks"] = Json::Value(Json::arrayValue);

	Json::Value& ids = request["tasks"];
	ids.resize(Json::ArrayIndex(tasks.size()));
	Json::ArrayIndex index = 0;
	for (TaskInfo& info : tasks) {
		ids[index] = info.task_id;
		index++;
	}
	
	std::string result = HttpUtil::Post(cluster->GetServerAddr() + "/api/v1/tasks", request.toStyledString());
	Json::Value response;
	if (!ParseJsonFromString(result, response))
		return false;
	if (response.type() == Json::objectValue) {
		LOG(ERROR) << "Unable to update task list for job " << job_spec.job_id << ": " << response["error"].asString();
		return false;
	}
	for (Json::ArrayIndex i = 0; i < response.size(); i++) {
		std::string task_id = response[i]["id"].asString();
		TaskInfoList::iterator it = std::find_if(tasks.begin(), tasks.end(), [&task_id](const TaskInfo& info) { return info.task_id == task_id; });
		if (it != tasks.end())
			FromJsonValue(*it, response[i]);
	}
	return true;
}

bool Job::TerminateTask(std::string task_id)
{
	Json::Value request(Json::objectValue);
	request["job_id"] = job_spec.job_id;
	request["task_id"] = task_id;
	std::string result = HttpUtil::Delete(cluster->GetServerAddr() + "/api/v1/tasks", request.toStyledString());
	Json::Value response;
	if (!ParseJsonFromString(result, response))
		return false;
	if (response.isMember("error")) {
		LOG(ERROR) << "Failed to terminate task " << job_spec.job_id << "." << task_id << ": " << response["error"].asString();
		return false;
	}
	return response["status"].asString() == "ok";
}

std::string Job::GetTaskStdOutput(std::string task_id)
{
	std::string request = boost::str(boost::format("%s/api/v1/tasks/stream?job_id=%s&task_id=%s&type=out")
		% cluster->GetServerAddr() % job_spec.job_id % task_id);
	std::string result = HttpUtil::Get(request);
	Json::Value response;
	if (!ParseJsonFromString(result, response))
		return false;
	if (response.isMember("error")) {
		LOG(ERROR) << "Failed to get task " << job_spec.job_id << "." << task_id << " stdout: " << response["error"].asString();
		return false;
	}
	return response["out"].asString();
}

std::string Job::GetTaskStdError(std::string task_id)
{
	std::string request = boost::str(boost::format("%s/api/v1/tasks/stream?job_id=%s&task_id=%s&type=err")
		% cluster->GetServerAddr() % job_spec.job_id % task_id);
	std::string result = HttpUtil::Get(request);
	Json::Value response;
	if (!ParseJsonFromString(result, response))
		return false;
	if (response.isMember("error")) {
		LOG(ERROR) << "Failed to get task " << job_spec.job_id << "." << task_id << " stderr: " << response["error"].asString();
		return false;
	}
	return response["err"].asString();
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
	if (!task_spec.resources.IsEmpty())
		spec["resources"] = ToJsonValue(task_spec.resources);
	return spec;
}

static void FromJsonValue(TaskSpec& spec, const Json::Value& value)
{
	spec.task_name    = value["name"].asString();
	spec.environments = value.isMember("envs") ? value["envs"].asString() : "";
	spec.labels       = value.isMember("labels") ? value["labels"].asString() : "";
	spec.command      = value.isMember("exec") ? value["exec"].asString() : "";
	spec.command_args = value.isMember("args") ? value["args"].asString() : "";
	spec.work_dir     = value.isMember("workdir") ? value["workdir"].asString() : "";
	spec.parallelism  = value.isMember("parallelism") ? value["parallelism"].asInt() : 1;
	if (value.isMember("resources"))
		FromJsonValue(spec.resources, value["resources"]);
}

static Json::Value ToJsonValue(const JobSpec& job_spec)
{
	Json::Value spec(Json::objectValue);
	spec["id"] = job_spec.job_id;
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
	if (!job_spec.resources.IsEmpty())
		spec["resources"] = ToJsonValue(job_spec.resources);
	Json::Value tasks(Json::arrayValue);
	for (auto t : job_spec.tasks)
		tasks.append(ToJsonValue(t));
	spec["tasks"] = tasks;
	return spec;
}

void FromJsonValue(JobSpec& spec, const Json::Value& value)
{
	spec.job_id       = value["id"].asString();
	spec.job_name     = value["name"].asString();
	spec.job_type     = ToJobType(value["type"].asCString());
	spec.environments = value.isMember("envs") ? value["envs"].asString() : "";
	spec.labels       = value.isMember("labels") ? value["labels"].asString() : "";
	spec.priority     = value["priority"].asInt();
	spec.command      = value.isMember("exec") ? value["exec"].asString() : "";
	spec.work_dir     = value.isMember("workdir") ? value["workdir"].asString() : "";
	if (value.isMember("resources"))
		FromJsonValue(spec.resources, value["resources"]);
	if (value.isMember("tasks")) {
		const Json::Value& task_jsons = value["tasks"];
		for (Json::ArrayIndex i = 0; i < task_jsons.size(); i++) {
			TaskSpec task_spec;
			FromJsonValue(task_spec, task_jsons[i]);
			spec.tasks.push_back(task_spec);
		}
	}
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

static void FromJsonValue(ResourceClaim& claim, const Json::Value& value)
{
	std::vector<std::string> names = value.getMemberNames();
	for (std::string& n : names) {
		if (value[n].isInt())
			claim.Set(n, value[n].asInt());
		else
			claim.Set(n, value[n].asFloat());
	}
}

static void FromJsonValue(TaskInfo& info, const Json::Value& value)
{
	info.task_id = value["id"].asString();
	info.task_name = value["name"].asString();
	info.task_state = ToTaskState(value["state"].asCString());
	info.progress = value["progress"].asInt();
	info.exec_node = value.isMember("node") ? value["node"].asString() : "";
	info.message = value.isMember("message") ? value["message"].asString() : "";
	info.start_time = value.isMember("start_time") ? value["start_time"].asInt64() : 0;
	info.finish_time = value.isMember("finish_time") ? value["finish_time"].asInt64() : 0;
	info.exit_code = value["exit_code"].asUInt();
}

static void FromJsonValue(NodeInfo& info, const Json::Value& value)
{
	info.name = value["name"].asString();
	info.address = value["address"].asString();
	info.os = value["os"].asString();
	info.state = ToMachineState(value["state"].asCString());
	info.online = value["online"].asInt64();
	info.labels = value.isMember("labels") ? value["labels"].asString() : "";
	if (value.isMember("resources"))
		FromJsonValue(info.resources, value["resources"]);
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

const char* ToString(MachineState state)
{
	if (state == MachineState::Online)
		return "Online";
	if (state == MachineState::Offline)
		return "Offline";
	return "Unknown";
}

MachineState ToMachineState(const char* state)
{
	if (strcmp(state, "Online") == 0)
		return MachineState::Online;
	if (strcmp(state, "Offline") == 0)
		return MachineState::Offline;
	return MachineState::Unknown;
}

}
