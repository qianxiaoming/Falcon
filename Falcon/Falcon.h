#ifndef FALCON_LIGHTWEIGHT_TASK_SCHEDULER_H
#define FALCON_LIGHTWEIGHT_TASK_SCHEDULER_H

#include <string>
#include <map>
#include <list>
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/format.hpp>
#include "json/json.h"

namespace falcon {

const unsigned short MASTER_SLAVE_PORT  = 36780;
const unsigned short MASTER_CLIENT_PORT = 36781;

#define FALCON_MASTER_SERVER_NAME "Falcon-Master"
#define FALCON_SLAVE_SERVER_NAME  "Falcon-Slave"

#define RESOURCE_CPU              "cpu"
#define RESOURCE_GPU              "gpu"
#define RESOURCE_MEM              "mem"
#define RESOURCE_DISK             "disk"

const float DEFAULT_CPU_USAGE  = 1.0;  // 1 cpu for each task
const int   DEFAULT_GPU_USAGE  = 0;    // no gpu used for each task
const int   DEFAULT_MEM_USAGE  = 256;  // 256M memory for each task
const int   DEFAULT_DISK_USAGE = 200;  // 200M local disk for each task

/**
* @brief Resource required by tasks
*/
struct Resource
{
	enum class Type {Int, Float};
	char   name[16];
	Type   type;
	union {
		int ival;
		float fval;
	} amount;

	Resource(const char* n, Type t, int a);
	Resource(const char* n, Type t, float a);
};
typedef std::map<std::string, Resource> ResourceMap;

struct Job;

/**
* @brief Task information to be scheduled and executed on machines
*/
struct Task
{
	enum class State { Queued, Dispatching, Executing, Completed, Failed, Aborted, Terminated };
	struct Status
	{
		Status() : state(State::Queued), exit_normal(true), exit_code(0) { }
		State state;
		bool  exit_normal;
		int   exit_code;
	};

	Task(std::string id, std::string name) : task_id(id), task_name(name) { }

	void Assign(const Json::Value& value, const Job& job);

	std::string task_id;
	Status      task_status;
	std::string task_name;
	std::string task_labels;
	std::string exec_command;
	std::string exec_args;
	std::string exec_envs;
	ResourceMap resources;
};
typedef boost::shared_ptr<Task> TaskPtr;
typedef std::list<TaskPtr>      TaskList;

struct Job
{
	enum class Type { Batch, DAG };
	enum class State { Queued, Waiting, Executing, Halted, Completed, Failed, Terminated };

	Job(std::string id, std::string name, Type type);

	virtual ~Job() { }

	virtual void Assign(const Json::Value& value);

	std::string job_id;
	std::string job_name;
	std::string job_labels;
	std::string job_envs;
	Type        job_type;
	ResourceMap resources;    // default resources for tasks
	
	time_t      submit_time;
	time_t      exec_time;
	time_t      finish_time;
	State       job_state;
};
typedef boost::shared_ptr<Job> JobPtr;
typedef std::list<JobPtr>      JobList;

const char* ToString(Job::Type type);
const char* ToString(Job::State state);
template <typename T> T FromString(const char* type);

struct BatchJob : public Job
{
	BatchJob(std::string id, std::string name)
		: Job(id, name, Type::Batch) { }

	virtual void Assign(const Json::Value& value);

	std::string exec_default;
	TaskList    exec_tasks;
};

struct DAGJob : public Job
{
	DAGJob(std::string id, std::string name)
		: Job(id, name, Type::DAG) { }

	virtual void Assign(const Json::Value& value);

	JobList exec_jobs;

	typedef std::map<JobPtr, JobList> DAGRelation;
	DAGRelation job_dag;
};

struct Machine
{
	enum class State { Online, Offline, Unknown };
	std::string name;
	std::string ip;
	std::string os;
	State       state;
	ResourceMap resources;

	TaskList    exec_tasks;
	ResourceMap availables;
	time_t      heartbeat;
};
typedef std::map<std::string, Machine> MachineMap;

}

#endif
