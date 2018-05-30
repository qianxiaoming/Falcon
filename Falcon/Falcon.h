#ifndef FALCON_LIGHTWEIGHT_TASK_SCHEDULER_H
#define FALCON_LIGHTWEIGHT_TASK_SCHEDULER_H

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

/**
* @brief Task information to be scheduled and executed on machines
*/
struct Task
{
	enum class State {Queued, Scheduling, Dispatching, Running, Finished};
	struct Status
	{
		State state;
		bool  exit_normal;
		int   exit_code;
	};
	Task(std::string name,  int parallel = 1) : task_name(name), num_parallel(parallel) { }

	std::string task_name;
	std::string labels;
	std::string exec_command;
	std::string exec_args;
	std::string environments;
	std::vector<Resource> resources;

	int num_parallel;
	std::vector<Status> status;
};

class Job
{

};

class BatchJob : public Job
{

};

class DAGJob : public Job
{
};

struct Machine
{

};

}

#endif
