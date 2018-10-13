#ifndef FALCON_API_H
#define FALCON_API_H

#include "CommonDef.h"

namespace falcon {
namespace api {

class Job;
struct JobSpec;
struct TaskSpec;
struct TaskInfo;
struct NodeInfo;
typedef std::shared_ptr<Job> JobPtr;
typedef std::list<JobPtr> JobList;
typedef std::list<TaskSpec> TaskSpecList;
typedef std::list<NodeInfo> NodeList;

class FALCON_API ComputingCluster
{
public:
	ComputingCluster(const std::string& server, uint16_t port);

	bool IsConnected() const;

	std::string GetName() const;

	bool SubmitJob(JobSpec& job_spec, std::string* errmsg = nullptr);

	bool TerminateJob(const std::string& id);

	JobPtr QueryJob(const std::string& id) const;

	JobList QueryJobList() const;

	NodeList GetNodeList() const;

private:
	std::string server_addr;
	std::string cluster_name;
	bool        connected;
};

struct FALCON_API TaskInfo
{

};

struct FALCON_API TaskSpec
{
	TaskSpec();
	TaskSpec(std::string name);
	TaskSpec(std::string name, std::string cmd, std::string cmd_args);

	std::string   task_name;
	std::string   command;
	std::string   command_args;
	std::string   environments;
	std::string   labels;
	std::string   work_dir;
	ResourceClaim resources;
	int           parallelism;
};

struct FALCON_API JobSpec
{
	JobSpec();
	JobSpec(JobType type);
	JobSpec(JobType type, std::string name, const ResourceClaim& claims);
	JobSpec(JobType type, std::string name, std::string cmd);
	JobSpec& AddTask(const TaskSpec& task);
	JobSpec& AddTask(std::string name, std::string cmd, std::string cmd_args);

	JobType       job_type;
	std::string   job_id;
	std::string   job_name;	
	std::string   environments;
	std::string   labels;
	int           priority;
	std::string   command;
	std::string   work_dir;
	ResourceClaim resources;
	TaskSpecList  tasks;
};

struct FALCON_API NodeInfo
{

};

}
}

#endif
