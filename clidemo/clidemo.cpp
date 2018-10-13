#include "stdafx.h"
#include <iostream>
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

	// submit batch job
	std::cout << "Submitting batch job..." << std::endl;
	// define resources needed by tasks
	ResourceClaim resources;
	// different methods for add task to job
	JobSpec job_spec(JobType::Batch, "Build Model", resources);
	job_spec.AddTask("Task-1", "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe", "task-1")
		    .AddTask(TaskSpec("Task-2", "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe", "task-2"));
	TaskSpec task3("Task-3");
	task3.command = "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe";
	task3.command_args = "";
	task3.environments = "TASK_NAME=Task-3";
	job_spec.AddTask(task3);

	// submit job
	std::string msg;
	if (!cluster.SubmitJob(job_spec, &msg)) {
		std::cerr << "Failed to submit batch job: " << msg << std::endl;
		return EXIT_FAILURE;
	}
	std::cout << "Success with job id: " << job_spec.job_id << std::endl;
	return EXIT_SUCCESS;
}
