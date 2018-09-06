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

Resource::Resource(const char* n, Type t, int a) : type(t)
{
	strcpy(name, n);
	amount.ival = a;
}

Resource::Resource(const char* n, Type t, float a) : type(t)
{
	strcpy(name, n);
	amount.fval = a;
}

void Task::Assign(const Json::Value& value, const Job& job)
{
	if (value.isMember("envs"))
		exec_envs = value["envs"].asString();
	else
		exec_envs = job.job_envs;

	if (value.isMember("labels"))
		task_labels = value["labels"].asString();
	else
		task_labels = job.job_labels;

	if (value.isMember("resources"))
		resources = Util::ParseResourcesJson(value["resources"]);
	else
		resources = job.resources;
}

Job::Job(std::string id, std::string name, Type type)
	: job_id(id), job_name(name), job_type(type),
	  submit_time(0), exec_time(0), finish_time(0), job_state(Job::State::Queued)
{
}

void Job::Assign(const Json::Value& value)
{
	if (value.isMember("envs"))
		job_envs = value["envs"].asString();
	if (value.isMember("labels"))
		job_labels = value["labels"].asString();
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
				TaskPtr task(new Task(id, v["name"].asString()));
				task->Assign(v, *this);
				if (task->exec_command.empty())
					task->exec_command = exec_default;
				exec_tasks.push_back(task);
			}
		} else {
			TaskPtr task(new Task(std::to_string(i), v["name"].asString()));
			task->Assign(v, *this);
			if (task->exec_command.empty())
				task->exec_command = exec_default;
			exec_tasks.push_back(task);
		}
	}
}

void DAGJob::Assign(const Json::Value& value)
{
	Job::Assign(value);
}

}