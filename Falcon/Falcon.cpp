#include "Falcon.h"
#include "Util.h"

namespace falcon {

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

template <>
JobType FromString(const char* type)
{
	if (strcmp(type, "Batch") == 0)
		return JobType::Batch;
	return JobType::DAG;
}

template <>
JobState FromString(const char* state)
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

template <>
TaskState FromString(const char* state)
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
			if (it->second.type == ResourceSet::ValueType::Int) {
				if (item.second.value.ival == 0)
					props.insert(std::make_pair(it->first, 0.0f));
				else
					props.insert(std::make_pair(it->first, float(it->second.value.ival) / item.second.value.ival));
			} else {
				if (item.second.value.fval == 0.0f)
					props.insert(std::make_pair(it->first, 0.0f));
				else
					props.insert(std::make_pair(it->first, it->second.value.fval / item.second.value.fval));
			}
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

std::string ResourceSet::ToString() const
{
	std::ostringstream oss;
	ResourceSet::const_iterator it = items.begin();
	while (it != items.end()) {
		oss << it->first << "=";
		if (it->second.type == ResourceSet::ValueType::Int)
			oss << it->second.value.ival << ";";
		else
			oss << it->second.value.fval << ";";
		it++;
	}
	return oss.str();
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

TaskStatus::TaskStatus()
	: state(TaskState::Queued), progress(0), exit_code(0), exec_time(0), finish_time(0)
{
}

TaskStatus::TaskStatus(TaskState s)
	: state(s), exit_code(0), exec_time(0), finish_time(0)
{
}

bool TaskStatus::IsFinished() const
{
	return state == TaskState::Completed || state == TaskState::Failed ||
		state == TaskState::Aborted || state == TaskState::Terminated;
}

Task::Task(const std::string& job_id, const std::string& task_id, std::string name)
	: job_id(job_id), task_id(task_id), task_name(name)
{
}

void Task::Assign(const Json::Value& value, const Job* job)
{
	if (value.isMember("args"))
		exec_args = value["args"].asString();

	if (value.isMember("exec"))
		exec_command = value["exec"].asString();

	if (value.isMember("workdir"))
		work_dir = value["workdir"].asString();
	else if (job)
		work_dir = job->work_dir;

	if (value.isMember("envs"))
		exec_envs = value["envs"].asString();
	else if (job)
		exec_envs = job->job_envs;

	if (value.isMember("labels"))
		task_labels = Util::ParseLabelList(value["labels"].asString());
	else if (job)
		task_labels = job->job_labels;

	if (value.isMember("resources"))
		resources = Util::ParseResourcesJson(value["resources"]);
	else if (job)
		resources = job->resources;
}

Json::Value Task::ToJson() const
{
	Json::Value v(Json::objectValue);
	v["name"] = task_name;
	v["exec"] = exec_command;
	v["args"] = exec_args;
	v["envs"] = exec_envs;
	v["workdir"] = work_dir;
	v["labels"] = ToString(task_labels);
	v["resources"] = resources.ToJson();
	return v;
}

Job::Job(std::string id, std::string name, JobType type)
	: job_id(id), job_name(name), job_type(type), job_priority(50), is_schedulable(true),
	  submit_time(0), exec_time(0), finish_time(0), job_state(JobState::Queued)
{
}

void Job::Assign(const Json::Value& value)
{
	if (value.isMember("workdir"))
		work_dir = value["workdir"].asString();
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
				task->Assign(v, this);
				if (task->exec_command.empty())
					task->exec_command = exec_default;
				exec_tasks.push_back(task);
			}
		} else {
			TaskPtr task(new Task(job_id, std::to_string(i), v["name"].asString()));
			task->Assign(v, this);
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

void BatchJob::GetTaskList(TaskList& tasks, TaskStatePred pred) const
{
	if (pred.empty())
		tasks.assign(exec_tasks.begin(), exec_tasks.end());
	else
		std::copy_if(exec_tasks.begin(), exec_tasks.end(), std::back_inserter(tasks), pred);
}

JobState BatchJob::UpdateCurrentState()
{
	int num_abort = 0, num_failed = 0, num_completed = 0, num_terminated = 0;
	for (TaskPtr t : exec_tasks) {
		TaskState task_state = t->task_status.state;
		if (task_state == TaskState::Executing) {
			job_state = JobState::Executing;
			return job_state;
		}
		else if (task_state == TaskState::Completed)
			num_completed++;
		else if (task_state == TaskState::Failed)
			num_failed++;
		else if (task_state == TaskState::Aborted)
			num_abort++;
		else if (task_state == TaskState::Terminated)
			num_terminated++;
	}
	if (num_abort > 0 || num_failed > 0) job_state = JobState::Failed;
	if (num_terminated > 0) job_state = JobState::Terminated;
	if (num_completed == int(exec_tasks.size())) job_state = JobState::Completed;
	return job_state;
}

void DAGJob::Assign(const Json::Value& value)
{
	Job::Assign(value);
}

TaskPtr DAGJob::GetTask(const std::string& id) const
{
	return TaskPtr();
}

void DAGJob::GetTaskList(TaskList& tasks, TaskStatePred pred) const
{
}

JobState DAGJob::UpdateCurrentState()
{
	return JobState::Queued;
}

}