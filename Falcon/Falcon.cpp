#include "Falcon.h"
#include "Util.h"

namespace falcon {

ResourceSet& ResourceSet::operator+=(const ResourceSet& other)
{
	for (auto& item : other.items) {
		auto it = items.find(item.first);
		if (it == items.end())
			items.insert(item);
		else {
			if (it->second.type == ResourceClaim::ValueType::Int)
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
			if (it->second.type == ResourceClaim::ValueType::Int)
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
			if (it->second.type == ResourceClaim::ValueType::Int) {
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
		if (it->second.type == ResourceClaim::ValueType::Int)
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
	if (it->second.type == ResourceClaim::ValueType::Int)
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
	ResourceClaim::const_iterator it = items.begin();
	while (it != items.end()) {
		if (it->second.type == ResourceClaim::ValueType::Int)
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
		ResourceClaim::const_iterator it = items.find(v.first);
		if (it == items.end())
			return false;
		if ((it->second.type == ValueType::Int   && it->second.value.ival < v.second.value.ival) ||
			(it->second.type == ValueType::Float && it->second.value.fval < v.second.value.fval))
			return false;
	}
	return true;
}

TaskStatus::TaskStatus()
	: state(TaskState::Queued), progress(0), exit_code(0), exec_time(0), finish_time(0)
{
}

TaskStatus::TaskStatus(TaskState s)
	: state(s), progress(0), exit_code(0), exec_time(0), finish_time(0)
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
	  submit_time(0), exec_time(0), finish_time(0), job_state(JobState::Queued), progress(0)
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

Json::Value Job::ToJson() const
{
	Json::Value val(Json::objectValue);
	val["id"] = job_id;
	val["name"] = job_name;
	val["type"] = ToString(job_type);
	if (!job_envs.empty())
		val["envs"] = job_envs;
	if (!job_labels.empty())
		val["labels"] = ToString(job_labels);
	val["priority"] = job_priority;
	if (!exec_default.empty())
		val["exec"] = exec_default;
	if (!work_dir.empty())
		val["workdir"] = work_dir;
	if (!resources.items.empty())
		val["resources"] = resources.ToJson();
	return val;
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
		if (v.isMember("parallelism")) {
			int count = v["parallelism"].asInt();
			for (int p = 0; p < count; p++) {
				std::string id = boost::str(boost::format("%d.%d") % (i + 1) % (p + 1));
				TaskPtr task(new Task(job_id, id, v["name"].asString()));
				task->Assign(v, this);
				if (task->exec_command.empty())
					task->exec_command = exec_default;
				if (!task->exec_envs.empty())
					task->exec_envs += ";";
				task->exec_envs += boost::str(boost::format("TASK_INDEX=%d") % p);
				exec_tasks.push_back(task);
			}
		} else {
			TaskPtr task(new Task(job_id, std::to_string((i+1)), v["name"].asString()));
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
	int num_exec = 0, num_abort = 0, num_failed = 0, num_completed = 0, num_terminated = 0;
	float accu_prog = 0.0f, scale_prog = 1.0f / exec_tasks.size();
	for (TaskPtr t : exec_tasks) {
		TaskState task_state = t->task_status.state;
		if (task_state == TaskState::Executing) {
			job_state = JobState::Executing;
			accu_prog += t->task_status.progress*scale_prog;
			num_exec++;
		} else if (task_state == TaskState::Completed)
			num_completed++;
		else if (task_state == TaskState::Failed)
			num_failed++;
		else if (task_state == TaskState::Aborted)
			num_abort++;
		else if (task_state == TaskState::Terminated)
			num_terminated++;
	}
	if (num_exec == 0) {
		if (num_abort > 0 || num_failed > 0) job_state = JobState::Failed;
		if (num_terminated > 0) job_state = JobState::Terminated;
		if (num_completed == int(exec_tasks.size())) job_state = JobState::Completed;
	}
	progress = int((num_completed + num_failed + num_abort + num_terminated) / float(exec_tasks.size()) * 100.0f + accu_prog);
	if (progress > 100)
		progress = 100;
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