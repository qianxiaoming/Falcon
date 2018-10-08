#ifndef FALCON_LIGHTWEIGHT_TASK_SCHEDULER_H
#define FALCON_LIGHTWEIGHT_TASK_SCHEDULER_H

#include <string>
#include <map>
#include <list>
#include <vector>
#include <boost/smart_ptr.hpp>
#include <boost/format.hpp>
#include <boost/function.hpp>
#include "json/json.h"
#include "CommonDef.h"

namespace falcon {

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
	std::string ToString() const;

	bool IsSatisfiable(const ResourceSet& other) const;
	
	typedef std::map<std::string, Value>::iterator iterator;
	typedef std::map<std::string, Value>::const_iterator const_iterator;
	std::map<std::string, Value> items;
};

typedef std::map<std::string, std::string> LabelList;
std::string ToString(const LabelList& labels);

struct Job;

struct TaskStatus
{
	TaskStatus();
	TaskStatus(TaskState s);
	bool IsFinished() const;

	TaskState   state;
	int         progress;
	std::string exec_tip;
	uint32_t    exit_code;
	std::string error_msg;
	time_t      exec_time;
	time_t      finish_time;
	std::string slave_id;
	std::string machine;
};

/**
* @brief Task information to be scheduled and executed on machines
*/
struct Task
{
	Task(const std::string& job_id, const std::string& task_id, std::string name);

	void Assign(const Json::Value& value, const Job* job);

	Json::Value ToJson() const;

	std::string job_id;
	std::string task_id;
	std::string task_name;
	std::string exec_command;
	std::string exec_args;
	std::string exec_envs;
	std::string work_dir;
	LabelList   task_labels;
	ResourceSet resources;

	TaskStatus  task_status;
};
typedef boost::shared_ptr<Task> TaskPtr;
typedef std::list<TaskPtr>      TaskList;
typedef boost::function<bool(const TaskPtr&)> TaskStatePred;

struct Job
{
	Job(std::string id, std::string name, JobType type);

	virtual ~Job() { }

	bool IsSchedulable() const { return is_schedulable; }
	void SetSchedulable(bool schedulable) { is_schedulable = schedulable; }
	virtual void Assign(const Json::Value& value);
	virtual TaskPtr GetTask(const std::string& id) const = 0;
	virtual void GetTaskList(TaskList& tasks, TaskStatePred pred = TaskStatePred()) const = 0;
	virtual JobState UpdateCurrentState() = 0;

	std::string job_id;
	std::string job_name;
	std::string job_envs;
	LabelList   job_labels;
	JobType     job_type;
	int         job_priority;
	std::string work_dir;
	ResourceSet resources;    // default resources for tasks
	bool        is_schedulable;
	
	time_t      submit_time;
	time_t      exec_time;
	time_t      finish_time;
	JobState    job_state;
};
typedef boost::shared_ptr<Job> JobPtr;
typedef std::list<JobPtr>      JobList;

struct BatchJob : public Job
{
	BatchJob(std::string id, std::string name)
		: Job(id, name, JobType::Batch) { }

	virtual void Assign(const Json::Value& value);
	virtual TaskPtr GetTask(const std::string& id) const;
	virtual void GetTaskList(TaskList& tasks, TaskStatePred pred = TaskStatePred()) const;
	virtual JobState UpdateCurrentState();

	std::string exec_default;
	TaskList    exec_tasks;
};

struct DAGJob : public Job
{
	DAGJob(std::string id, std::string name)
		: Job(id, name, JobType::DAG) { }

	virtual void Assign(const Json::Value& value);
	virtual TaskPtr GetTask(const std::string& id) const;
	virtual void GetTaskList(TaskList& tasks, TaskStatePred pred = TaskStatePred()) const;
	virtual JobState UpdateCurrentState();

	JobList exec_jobs;

	typedef std::map<JobPtr, JobList> DAGRelation;
	DAGRelation job_dag;
};

struct Machine
{
	std::string    id;
	std::string    name;
	std::string    address;
	uint16_t       port;
	std::string    os;
	int            cpu_count;
	int            cpu_frequency;
	MachineState   state;
	LabelList      labels;
	ResourceSet    resources;

	TaskList       exec_tasks;
	ResourceSet    availables;
	time_t         online;
	time_t         heartbeat;
};
typedef boost::shared_ptr<Machine> MachinePtr;
typedef std::map<std::string, MachinePtr> MachineMap;

const char* ToString(JobType type);
const char* ToString(JobState state);
const char* ToString(TaskState state);
template <typename T> T FromString(const char* type);

}

#endif
