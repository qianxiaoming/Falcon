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
	std::string addr = "127.0.0.1";
	if (argc > 1)
		addr = argv[1];
	// 连接到集群
	ComputingCluster cluster(addr, port);
	if (!cluster.IsConnected()) {
		std::cerr << "Failed to connect to " << addr << std::endl;
		return EXIT_FAILURE;
	}
	std::cout << "Cluster \"" << cluster.GetName() << "\" connected." << std::endl;

	// 获取所有节点信息
	std::cout << "Nodes in the cluster:" << std::endl;
	NodeList nodes = cluster.GetNodeList();
	for (NodeInfo& node : nodes) {
		std::cout << "  Node Name: " << node.name << std::endl;
		std::cout << "  Address:   " << node.address << ":" << node.port << std::endl;
		std::cout << "  OS Name:   " << node.os << std::endl;
		std::cout << "  State:     " << ToString(node.state) << std::endl;
		std::cout << "  Resources: " << node.resources.ToString() << std::endl;
	}
	std::cout << std::endl;

	// 提交一个批处理作业（含4个任务）
	std::cout << "Submitting batch job..." << std::endl;
	// 定义默认的任务资源需求：1个CPU，0个GPU，1G内存使用
	ResourceClaim resources;
	resources.Set(RESOURCE_CPU, 1);
	JobSpec job_spec(JobType::Batch, "Build Model", resources);
	job_spec.command = "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe"; // 所有任务默认使用的可执行程序
	for (int i = 0; i < 16; i++) {
		char name[256] = { 0 }, args[256] = { 0 };
		sprintf(name, "Task %d", i);
		sprintf(args, "data file %d", i);
		job_spec.AddTask(name, job_spec.command, args);
	}
	// 以下演示不同的方法向Job中增加Task
	//// 使用AddTask可以连续追加Task。当没有特别资源需求时，可以使用简便的方法
	//job_spec.AddTask("CPU Task-1", "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe", "some-file")
	//	    .AddTask(TaskSpec("CPU Task-2", "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe", "some-file"));
	//// 使用单独的TaskSpec对象，可以单独指定程序名称，环境变量等
	//TaskSpec task3("CPU Task-3");
	//task3.command_args = "some file for task3";
	//task3.environments = "TASK_NAME=Task-3";
	//job_spec.AddTask(task3);
	//// 定义一个使用GPU的任务
	//TaskSpec task4("GPU Task-1", "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe", "some-file");
	//task4.work_dir = "C:\\Temp"; // 指定程序的工作目录
	//// 指定程序启动多少个实例，默认是1个。注意当启动多个实例时，可执行程序和参数都是一样的，
	//// 可通过环境变量TASK_INDEX区分（从0开始）
	//task4.parallelism = 4;
	//task4.resources.Set(RESOURCE_CPU, 2);
	//task4.resources.Set(RESOURCE_GPU, 1); // 使用1个GPU
	//job_spec.AddTask(task4);

	// 提交作业到集群。提交成功后，job_spec里面的job_id被更新为作业的唯一标识。
	std::string msg;
	if (!cluster.SubmitJob(job_spec, &msg)) {
		std::cerr << "Failed to submit batch job: " << msg << std::endl;
		return EXIT_FAILURE;
	}
	std::cout << "Success with job id: " << job_spec.job_id << std::endl;

	// 获取所有作业并输出
	std::cout << std::endl << "All jobs in cluster:" << std::endl;
	JobList jobs = cluster.QueryJobList();
	for (JobPtr job : jobs) {
		const JobSpec& job_spec = job->GetSpec();
		std::cout << job_spec.job_id << "\t" << job_spec.job_name << "\t" << ToString(job_spec.job_type) << std::endl;
	}
	if (jobs.empty())
		return EXIT_FAILURE;

	// 下面使用最后一个job作为演示的例子
	JobPtr job = *(jobs.rbegin());
	TaskInfoList tasks = job->GetTaskList(); // 获取作业中的所有Task信息
	while (true) {
		_sleep(2000); // 周期性刷新状态信息
		JobInfo info;
		job->UpdateJobInfo(info); // 更新作业的状态信息并显示
		std::cout << std::endl << "JobID: " << job->GetSpec().job_id << "\tState: "<<ToString(info.job_state)<<"\tProgress: " << info.progress << std::endl;

		job->UpdateTaskInfo(tasks); // 更新Task状态信息，传入开始获取到的Task列表
		if (tasks.empty())
			break; // 注意：UpdateTaskInfo函数内部会移除上一次更新时已经结束的Task（无论成功还是失败），因此当列表为空就表示所有的Task都结束了
		for (TaskInfo& t : tasks) {
			// 输出Task的状态信息
			std::cout << " ID=" << t.task_id << "\t\tName=" << t.task_name << "\tState=" << ToString(t.task_state) << "\tProgress=" << t.progress << "%\tNode="
				<< (t.exec_node.empty() ? "----" : t.exec_node) << "\tExitCode=" << t.exit_code;
			if (t.start_time && t.finish_time)
				std::cout << "\tDuration=" << (t.finish_time - t.start_time) << "s";
			std::cout << std::endl;
			if (t.IsFinished()) {
				// 任务已经结束了，输出标准流的内容
				_sleep(2000);
				std::string out = job->GetTaskStdOutput(t.task_id).substr(0, 20);
				std::string err = job->GetTaskStdError(t.task_id).substr(0, 20);
				std::cout << "  stdout=" << out << std::endl;
				std::cout << "  stderr=" << err << std::endl;
			}
		}
	}
	
	return EXIT_SUCCESS;
}
