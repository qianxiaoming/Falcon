#include "stdafx.h"
#include <iostream>
#include <ctime>
#include "libfalcon.h"

int main(int argc, char* argv[])
{
	using namespace falcon;
	using namespace falcon::api;

	int port = MASTER_CLIENT_PORT;
	if (argc > 2)
		port = std::atoi(argv[2]);
	// connect to cluster
	ComputingCluster cluster(argv[1], port);
	if (!cluster.IsConnected()) {
		std::cerr << "Failed to connect to " << argv[1] << std::endl;
		return EXIT_FAILURE;
	}
	std::cout << "Cluster \"" << cluster.GetName() << "\" connected." << std::endl;

	// get nodes
	std::cout << "Nodes in the cluster:" << std::endl;
	NodeList nodes = cluster.GetNodeList();
	for (NodeInfo& node : nodes) {
		std::cout << "  Node Name: " << node.name << std::endl;
		std::cout << "  Address:   " << node.address << std::endl;
		std::cout << "  OS Name:   " << node.os << std::endl;
		std::cout << "  State:     " << ToString(node.state) << std::endl;
		std::cout << "  Resources: " << node.resources.ToString() << std::endl;
	}
	std::cout << std::endl;

	// submit batch job with 4 tasks
	std::cout << "Submitting batch job..." << std::endl;
	// define resources needed by tasks
	ResourceClaim resources;
	// different methods for add task to job
	JobSpec job_spec(JobType::Batch, "Build Model", resources);
	job_spec.command = "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe";
	job_spec.AddTask("CPU Task-1", "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe", "some-file")
		    .AddTask(TaskSpec("Task-2", "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe", "some-file"));
	TaskSpec task3("CPU Task-3"); // if you do not set command in TaskSpec, it will use the command of JobSpec
	task3.command_args = "some file for task3";
	task3.environments = "TASK_NAME=Task-3";
	job_spec.AddTask(task3);
	// define a task request gpu resource
	TaskSpec task4("GPU Task", "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe", "some-file");
	task4.work_dir = "C:\\Temp"; // specify the current dir for task execution
	task4.parallelism = 4; // 4 task instances to execute. Default is 1
	task4.resources.Set(RESOURCE_GPU, 1); // request 1 GPU
	job_spec.AddTask(task4);

	// submit job
	std::string msg;
	if (!cluster.SubmitJob(job_spec, &msg)) {
		std::cerr << "Failed to submit batch job: " << msg << std::endl;
		return EXIT_FAILURE;
	}
	std::cout << "Success with job id: " << job_spec.job_id << std::endl;

	// get all jobs
	std::cout << std::endl << "All jobs in cluster:" << std::endl;
	JobList jobs = cluster.QueryJobList();
	for (JobPtr job : jobs) {
		const JobSpec& job_spec = job->GetSpec();
		std::cout << job_spec.job_id << "\t" << job_spec.job_name << "\t" << job_spec.command << std::endl;
	}
	if (jobs.empty())
		return EXIT_FAILURE;

	// use the first job as example
	std::cout << std::endl;
	JobPtr job = *(jobs.begin());
	TaskInfoList tasks = job->GetTaskList(); // get all tasks in this job
	while (true) {
		_sleep(2000);
		JobInfo info;
		job->UpdateJobInfo(info);
		std::cout << "JobID: " << job->GetSpec().job_id << "\tState: "<<ToString(info.job_state)<<"\tProgress: " << info.progress << std::endl;

		job->UpdateTaskInfo(tasks);
		if (tasks.empty())
			break; // this means all tasks in this job are finished
		for (TaskInfo& t : tasks) {
			std::cout << "  " << t.task_id << "\t" << t.task_name << "\t" << ToString(t.task_state) << "\t" << t.progress << "\t" << t.exec_node << "\t";
			if (t.start_time)
				std::cout << std::ctime(&t.start_time) << "\t";
			if (t.finish_time)
				std::cout << std::ctime(&t.finish_time) << std::endl;
		}
	}
	
	return EXIT_SUCCESS;
}
