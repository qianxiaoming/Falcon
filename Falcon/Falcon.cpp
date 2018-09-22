#include "Falcon.h"
#include "Util.h"

namespace falcon {

const char* ToString(Task::State state)
{
	switch (state) {
	case Task::State::Queued:
		return "Queued";
	case Task::State::Dispatching:
		return "Dispatching";
	case Task::State::Executing:
		return "Executing";
	case Task::State::Completed:
		return "Completed";
	case Task::State::Failed:
		return "Failed";
	case Task::State::Aborted:
		return "Aborted";
	case Task::State::Terminated:
		return "Terminated";
	default:
		assert(false);
	}
	return "";
}

const char* ToString(Job::Type type)
{
	switch (type) {
	case Job::Type::Batch:
		return "Batch";
	case Job::Type::DAG:
		return "DAG";
	default:
		assert(false);
	}
	return "";
}

const char* ToString(Job::State state)
{
	switch (state) {
	case Job::State::Queued:
		return "Queued";
	case Job::State::Waiting:
		return "Waiting";
	case Job::State::Executing:
		return "Executing";
	case Job::State::Halted:
		return "Halted";
	case Job::State::Completed:
		return "Completed";
	case Job::State::Failed:
		return "Failed";
	case Job::State::Terminated:
		return "Terminated";
	default:
		assert(false);
	}
	return "";
}

template <>
Job::Type FromString(const char* type)
{
	if (strcmp(type, "Batch") == 0)
		return Job::Type::Batch;
	return Job::Type::DAG;
}

template <>
Job::State FromString(const char* state)
{
	if (strcmp(state, "Queued") == 0)
		return Job::State::Queued;
	if (strcmp(state, "Waiting") == 0)
		return Job::State::Waiting;
	if (strcmp(state, "Executing") == 0)
		return Job::State::Executing;
	if (strcmp(state, "Halted") == 0)
		return Job::State::Halted;
	if (strcmp(state, "Completed") == 0)
		return Job::State::Completed;
	if (strcmp(state, "Failed") == 0)
		return Job::State::Failed;
	return Job::State::Terminated;
}

template <>
Task::State FromString(const char* state)
{
	if (strcmp(state, "Queued") == 0)
		return Task::State::Queued;
	if (strcmp(state, "Dispatching") == 0)
		return Task::State::Dispatching;
	if (strcmp(state, "Executing") == 0)
		return Task::State::Executing;
	if (strcmp(state, "Completed") == 0)
		return Task::State::Completed;
	if (strcmp(state, "Failed") == 0)
		return Task::State::Failed;
	if (strcmp(state, "Aborted") == 0)
		return Task::State::Aborted;
	if (strcmp(state, "Terminated") == 0)
		return Task::State::Terminated;
	return Task::State::Queued;
}

ResourceSet::ResourceSet()
{
	items.swap(std::map<std::string, Value>(
	{ { RESOURCE_CPU,  DEFAULT_CPU_USAGE }, { RESOURCE_FREQ,  DEFAULT_FREQ_USAGE },
	  { RESOURCE_GPU,  DEFAULT_GPU_USAGE }, { RESOURCE_MEM,  DEFAULT_MEM_USAGE },
	  { RESOURCE_DISK, DEFAULT_DISK_USAGE} }));
}

bool ResourceSet::Exists(const std::string& name) const
{
	return items.find(name) != items.end();
}

bool ResourceSet::Exists(const char* name) const
{
	return items.find(name) != items.end();
}

template <typename T>
ResourceSet& SetResourceValue(ResourceSet& res, const std::string& name, T val)
{
	auto it = res.items.find(name);
	if (it == res.items.end())
		res.items.insert(std::make_pair(name, val));
	else {
		if (it->second.type == ResourceSet::ValueType::Int)
			it->second.value.ival = int(val);
		else
			it->second.value.fval = float(val);
	}
	return res;
}

ResourceSet& ResourceSet::Set(const std::string& name, int val)
{
	return SetResourceValue(*this, name, val);
}

ResourceSet& ResourceSet::Set(const std::string& name, float val)
{
	return SetResourceValue(*this, name, val);
}

template <typename T>
T GetResourceValue(const ResourceSet& res, const std::string& name, T val)
{
	auto it = res.items.find(name);
	if (it == res.items.end())
		return val;
	if (it->second.type == ResourceSet::ValueType::Int)
		return T(it->second.value.ival);
	else
		return T(it->second.value.ival);
}

int ResourceSet::Get(const std::string& name, int val) const
{
	return GetResourceValue(*this, name, val);
}

float ResourceSet::Get(const std::string& name, float val) const
{
	return GetResourceValue(*this, name, val);
}

ResourceSet& ResourceSet::operator+=(const ResourceSet& other)
{
	for (auto& item : other.items) {
		auto it = items.find(item.first);
		if (it == items.end())
			items.insert(item);
		else {
			if (it->second.type == ResourceSet::ValueType::Int)
				it->second.value.ival += item.second.value.ival;
			else
				it->second.value.fval += item.second.value.fval;
		}
	}
	return *this;
}

ResourceSet& ResourceSet::operator-=(const ResourceSet& other)
{
	for (auto& item : other.items) {
		auto it = items.find(item.first);
		if (it != items.end()) {
			if (it->second.type == ResourceSet::ValueType::Int)
				it->second.value.ival -= item.second.value.ival;
			else
				it->second.value.fval -= item.second.value.fval;
		}
	}
	return *this;
}

ResourceSet::Proportion ResourceSet::operator/(const ResourceSet& other) const
{
	Proportion props;
	for (auto& item : other.items) {
		auto it = items.find(item.first);
		if (it != items.end()) {
			if (it->second.type == ResourceSet::ValueType::Int)
				props.insert(std::make_pair(it->first, float(it->second.value.ival) / item.second.value.ival));
			else
				props.insert(std::make_pair(it->first, it->second.value.fval / item.second.value.fval));
		}
	}
	return props;
}

template <typename T>
ResourceSet& IncResourceValue(ResourceSet& res, const std::string& name, T val)
{
	auto it = res.items.find(name);
	if (it == res.items.end())
		res.items.insert(std::make_pair(name, val));
	else {
		if (it->second.type == ResourceSet::ValueType::Int)
			it->second.value.ival += int(val);
		else
			it->second.value.fval += float(val);
	}
	return res;
}

ResourceSet& ResourceSet::Increase(const std::string& name, int val)
{
	return IncResourceValue(*this, name, val);
}

ResourceSet& ResourceSet::Increase(const std::string& name, float val)
{
	return IncResourceValue(*this, name, val);
}

template <typename T>
ResourceSet& DecResourceValue(ResourceSet& res, const std::string& name, T val)
{
	auto it = res.items.find(name);
	if (it == res.items.end())
		return res;
	if (it->second.type == ResourceSet::ValueType::Int)
		it->second.value.ival -= int(val);
	else
		it->second.value.fval -= float(val);
	return res;
}

ResourceSet& ResourceSet::Decrease(const std::string& name, int val)
{
	return DecResourceValue(*this, name, val);
}

ResourceSet& ResourceSet::Decrease(const std::string& name, float val)
{
	return DecResourceValue(*this, name, val);
}

Json::Value ResourceSet::ToJson() const
{
	Json::Value val(Json::objectValue);
	ResourceSet::const_iterator it = items.begin();
	while (it != items.end()) {
		if (it->second.type == ResourceSet::ValueType::Int)
			val[it->first] = it->second.value.ival;
		else
			val[it->first] = it->second.value.fval;
		it++;
	}
	return val;
}

bool ResourceSet::IsSatisfiable(const ResourceSet& other) const
{
	for (const auto& v : other.items) {
		ResourceSet::const_iterator it = items.find(v.first);
		if (it == items.end())
			return false;
		if ((it->second.type == ValueType::Int   && it->second.value.ival < v.second.value.ival) ||
			(it->second.type == ValueType::Float && it->second.value.fval < v.second.value.fval))
			return false;
	}
	return true;
}

Task::Task(const std::string& job_id, const std::string& task_id, std::string name)
	: job_id(job_id), task_id(task_id), task_name(name)
{
}

void Task::Assign(const Json::Value& value, const Job& job)
{
	if (value.isMember("envs"))
		exec_envs = value["envs"].asString();
	else
		exec_envs = job.job_envs;

	if (value.isMember("labels"))
		task_labels = Util::ParseLabelList(value["labels"].asString());
	else
		task_labels = job.job_labels;

	if (value.isMember("resources"))
		resources = Util::ParseResourcesJson(value["resources"]);
	else
		resources = job.resources;
}

Json::Value Task::ToJson() const
{
	return Json::Value();
}

Job::Job(std::string id, std::string name, Type type)
	: job_id(id), job_name(name), job_type(type), job_priority(50),
	  submit_time(0), exec_time(0), finish_time(0), job_state(Job::State::Queued)
{
}

void Job::Assign(const Json::Value& value)
{
	if (value.isMember("envs"))
		job_envs = value["envs"].asString();
	if (value.isMember("labels"))
		job_labels = Util::ParseLabelList(value["labels"].asString());
	if (value.isMember("priority"))
		job_priority = value["priority"].asInt();
	if (value.isMember("resources"))
		resources = Util::ParseResourcesJson(value["resources"]);
}

void BatchJob::Assign(const Json::Value& value)
{
	Job::Assign(value);

	if (value.isMember("exec"))
		exec_default = value["exec"].asString();

	// parse task definitions
	const Json::Value& tasks = value["tasks"];
	for (Json::ArrayIndex i = 0; i < tasks.size(); i++) {
		const Json::Value& v = tasks[i];
		if (v.isMember("parallel")) {
			int count = v["parallel"].asInt();
			for (int p = 0; p < count; p++) {
				std::string id = boost::str(boost::format("%d.%d") % (i + 1) % (p + 1));
				TaskPtr task(new Task(job_id, id, v["name"].asString()));
				task->Assign(v, *this);
				if (task->exec_command.empty())
					task->exec_command = exec_default;
				exec_tasks.push_back(task);
			}
		} else {
			TaskPtr task(new Task(job_id, std::to_string(i), v["name"].asString()));
			task->Assign(v, *this);
			if (task->exec_command.empty())
				task->exec_command = exec_default;
			exec_tasks.push_back(task);
		}
	}
}

TaskPtr BatchJob::GetTask(const std::string& id) const
{
	TaskList::const_iterator it = std::find_if(exec_tasks.begin(), exec_tasks.end(), 
		[&id](const TaskPtr& t) { return t->task_id == id; });
	if (it == exec_tasks.end())
		return TaskPtr();
	return *it;
}

bool BatchJob::NextTask(TaskList::iterator& it)
{
	if (it == TaskList::iterator())
		it = exec_tasks.begin();
	else
		it++;
	return it != exec_tasks.end();
}

void DAGJob::Assign(const Json::Value& value)
{
	Job::Assign(value);
}

TaskPtr DAGJob::GetTask(const std::string& id) const
{
	return TaskPtr();
}

bool DAGJob::NextTask(TaskList::iterator& it)
{
	return false;
}

}