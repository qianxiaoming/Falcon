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
const unsigned short SLAVE_LISTEN_PORT  = 36782;

#define FALCON_MASTER_SERVER_NAME "Falcon-Master"
#define FALCON_SLAVE_SERVER_NAME  "Falcon-Slave"

#define RESOURCE_CPU              "cpu"
#define RESOURCE_FREQ             "freq"
#define RESOURCE_GPU              "gpu"
#define RESOURCE_MEM              "mem"
#define RESOURCE_DISK             "disk"

const float DEFAULT_CPU_USAGE  = 1.0;  // 1 cpu for each task
const int   DEFAULT_FREQ_USAGE = 2400; // 2.4GHz for each task
const int   DEFAULT_GPU_USAGE  = 0;    // no gpu used for each task
const int   DEFAULT_MEM_USAGE  = 256;  // 256M memory for each task
const int   DEFAULT_DISK_USAGE = 200;  // 200M local disk for each task

/**
* @brief Resources required by tasks or owned by machines
*/
struct ResourceSet
{
	enum class ValueType {Int, Float};
	struct Value
	{
		ValueType type;
		union {
			int ival;
			float fval;
		} value;

		Value() : type(ValueType::Int) { value.ival = 0; }
		Value(int v) : type(ValueType::Int) { value.ival = v; }
		Value(float v) : type(ValueType::Float) { value.fval = v; }
	};

	ResourceSet();

	bool Exists(const std::string& name) const;
	bool Exists(const char* name) const;

	ResourceSet& Set(const std::string& name, int val);
	ResourceSet& Set(const std::string& name, float val);

	int Get(const std::string& name, int val) const;
	float Get(const std::string& name, float val) const;

	ResourceSet& operator+=(const ResourceSet& other);
	ResourceSet& operator-=(const ResourceSet& other);
	
	typedef std::map<std::string, float> Proportion;
	Proportion operator/(const ResourceSet& other) const;

	ResourceSet& Increase(const std::string& name, int val);
	ResourceSet& Increase(const std::string& name, float val);
	ResourceSet& Decrease(const std::string& name, int val);
	ResourceSet& Decrease(const std::string& name, float val);

	Json::Value ToJson() const;

	bool IsSatisfiable(const ResourceSet& other) const;
	
	typedef std::map<std::string, Value>::iterator iterator;
	typedef std::map<std::string, Value>::const_iterator const_iterator;
	std::map<std::string, Value> items;
};

typedef std::map<std::string, std::string> LabelList;

struct Job;

/**
* @brief Task information to be scheduled and executed on machines
*/
struct Task
{
	enum class State { Queued, Dispatching, Executing, Completed, Failed, Aborted, Terminated };
	struct Status
	{
		Status() : state(State::Queued), exit_code(0), exec_time(0), finish_time(0) { }
		Status(State s) : state(s), exit_code(0), exec_time(0), finish_time(0) { }
		Status(State s, time_t exec_time, std::string mac) : state(s), exit_code(0), exec_time(exec_time), finish_time(0), machine(mac) { }
		Status(State s, time_t finish_time, int code) : state(s), exit_code(code), exec_time(0), finish_time(finish_time) { }
		State       state;
		int         exit_code;
		time_t      exec_time;
		time_t      finish_time;
		std::string machine;
	};

	Task(const std::string& job_id, const std::string& task_id, std::string name);

	void Assign(const Json::Value& value, const Job& job);

	Json::Value ToJson() const;

	std::string job_id;
	std::string task_id;
	std::string task_name;
	std::string exec_command;
	std::string exec_args;
	std::string exec_envs;
	LabelList   task_labels;
	ResourceSet resources;

	Status      task_status;
};
typedef boost::shared_ptr<Task> TaskPtr;
typedef std::list<TaskPtr>      TaskList;

const char* ToString(Task::State state);

struct Job
{
	enum class Type { Batch, DAG };
	enum class State { Queued, Waiting, Executing, Halted, Completed, Failed, Terminated };

	Job(std::string id, std::string name, Type type);

	virtual ~Job() { }

	virtual void Assign(const Json::Value& value);
	virtual TaskPtr GetTask(const std::string& id) const = 0;
	virtual bool NextTask(TaskList::iterator& it) = 0;

	std::string job_id;
	std::string job_name;
	std::string job_envs;
	LabelList   job_labels;
	Type        job_type;
	int         job_priority;
	ResourceSet resources;    // default resources for tasks
	
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
	virtual TaskPtr GetTask(const std::string& id) const;
	virtual bool NextTask(TaskList::iterator& it);

	std::string exec_default;
	TaskList    exec_tasks;
};

struct DAGJob : public Job
{
	DAGJob(std::string id, std::string name)
		: Job(id, name, Type::DAG) { }

	virtual void Assign(const Json::Value& value);
	virtual TaskPtr GetTask(const std::string& id) const;
	virtual bool NextTask(TaskList::iterator& it);

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
	int         cpu_count;
	int         cpu_frequency;
	State       state;
	LabelList   labels;
	ResourceSet resources;

	TaskList    exec_tasks;
	ResourceSet availables;
	time_t      online;
	time_t      heartbeat;
};
typedef std::map<std::string, Machine> MachineMap;

}

#endif
