#ifndef FALCON_LIGHTWEIGHT_TASK_SCHEDULER_H
#define FALCON_LIGHTWEIGHT_TASK_SCHEDULER_H

#include <string>
#include <map>
#include <list>
#include <vector>
#include <boost/smart_ptr.hpp>

namespace falcon {

const unsigned short MASTER_SLAVE_PORT  = 36780;
const unsigned short MASTER_CLIENT_PORT = 36781;

#define FALCON_MASTER_SERVER_NAME "Falcon-Master"
#define FALCON_SLAVE_SERVER_NAME  "Falcon-Slave"

#define RESOURCE_CPU              "cpu"
#define RESOURCE_GPU              "gpu"
#define RESOURCE_MEM              "mem"
#define RESOURCE_DISK             "disk"

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
};
typedef std::map<std::string, Resource> ResourceMap;

/**
* @brief Task information to be scheduled and executed on machines
*/
class Task
{
public:
	enum class State { Queued, Scheduling, Dispatching, Executing, Completed, Failed, Aborted, Terminated };
	struct Status
	{
		State state;
		bool  exit_normal;
		int   exit_code;
	};
	virtual ~Task() { }

protected:
	Task(int id, std::string name) : task_id(id), task_name(name) { }

	int         task_id;
	Status      task_status;
	std::string task_name;
	std::string task_labels;
	std::string exec_command;
	std::string exec_args;
	std::string exec_envs;
	ResourceMap resources;
};
typedef boost::shared_ptr<Task> TaskPtr;
typedef std::vector<TaskPtr>    TaskList;

class Job
{
public:
	enum class Type { Batch, DAG };
	enum class State { Queued, Waiting, Executing, Halted, Completed, Failed, Terminated };

	virtual ~Job() { }

protected:
	Job(std::string id, std::string name, Type type)
		: job_id(id), job_name(name), job_type(type) { }

	std::string job_id;
	std::string job_name;
	std::string job_labels;
	std::string job_envs;
	Type        job_type;
	
	time_t      submit_time;
	time_t      exec_time;
	time_t      finish_time;
	State       job_state;
};
typedef boost::shared_ptr<Job> JobPtr;
typedef std::list<JobPtr>      JobList;

const char* ToString(Job::Type type);
const char* ToString(Job::State state);

class BatchJob : public Job
{
public:
	BatchJob(std::string id, std::string name)
		: Job(id, name, Type::Batch) { }

private:
	std::string exec_default;
	TaskList    exec_tasks;
};

class DAGJob : public Job
{
public:
	DAGJob(std::string id, std::string name)
		: Job(id, name, Type::DAG) { }

private:
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
