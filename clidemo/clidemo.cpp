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
	// ���ӵ���Ⱥ
	ComputingCluster cluster(addr, port);
	if (!cluster.IsConnected()) {
		std::cerr << "Failed to connect to " << addr << std::endl;
		return EXIT_FAILURE;
	}
	std::cout << "Cluster \"" << cluster.GetName() << "\" connected." << std::endl;

	// ��ȡ���нڵ���Ϣ
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

	// �ύһ����������ҵ����4������
	std::cout << "Submitting batch job..." << std::endl;
	// ����Ĭ�ϵ�������Դ����1��CPU��0��GPU��1G�ڴ�ʹ��
	ResourceClaim resources;
	resources.Set(RESOURCE_CPU, 1);
	JobSpec job_spec(JobType::Batch, "Build Model", resources);
	job_spec.command = "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe"; // ��������Ĭ��ʹ�õĿ�ִ�г���
	for (int i = 0; i < 16; i++) {
		char name[256] = { 0 }, args[256] = { 0 };
		sprintf(name, "Task %d", i);
		sprintf(args, "data file %d", i);
		job_spec.AddTask(name, job_spec.command, args);
	}
	// ������ʾ��ͬ�ķ�����Job������Task
	//// ʹ��AddTask��������׷��Task����û���ر���Դ����ʱ������ʹ�ü��ķ���
	//job_spec.AddTask("CPU Task-1", "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe", "some-file")
	//	    .AddTask(TaskSpec("CPU Task-2", "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe", "some-file"));
	//// ʹ�õ�����TaskSpec���󣬿��Ե���ָ���������ƣ�����������
	//TaskSpec task3("CPU Task-3");
	//task3.command_args = "some file for task3";
	//task3.environments = "TASK_NAME=Task-3";
	//job_spec.AddTask(task3);
	//// ����һ��ʹ��GPU������
	//TaskSpec task4("GPU Task-1", "C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe", "some-file");
	//task4.work_dir = "C:\\Temp"; // ָ������Ĺ���Ŀ¼
	//// ָ�������������ٸ�ʵ����Ĭ����1����ע�⵱�������ʵ��ʱ����ִ�г���Ͳ�������һ���ģ�
	//// ��ͨ����������TASK_INDEX���֣���0��ʼ��
	//task4.parallelism = 4;
	//task4.resources.Set(RESOURCE_CPU, 2);
	//task4.resources.Set(RESOURCE_GPU, 1); // ʹ��1��GPU
	//job_spec.AddTask(task4);

	// �ύ��ҵ����Ⱥ���ύ�ɹ���job_spec�����job_id������Ϊ��ҵ��Ψһ��ʶ��
	std::string msg;
	if (!cluster.SubmitJob(job_spec, &msg)) {
		std::cerr << "Failed to submit batch job: " << msg << std::endl;
		return EXIT_FAILURE;
	}
	std::cout << "Success with job id: " << job_spec.job_id << std::endl;

	// ��ȡ������ҵ�����
	std::cout << std::endl << "All jobs in cluster:" << std::endl;
	JobList jobs = cluster.QueryJobList();
	for (JobPtr job : jobs) {
		const JobSpec& job_spec = job->GetSpec();
		std::cout << job_spec.job_id << "\t" << job_spec.job_name << "\t" << ToString(job_spec.job_type) << std::endl;
	}
	if (jobs.empty())
		return EXIT_FAILURE;

	// ����ʹ�����һ��job��Ϊ��ʾ������
	JobPtr job = *(jobs.rbegin());
	TaskInfoList tasks = job->GetTaskList(); // ��ȡ��ҵ�е�����Task��Ϣ
	while (true) {
		_sleep(2000); // ������ˢ��״̬��Ϣ
		JobInfo info;
		job->UpdateJobInfo(info); // ������ҵ��״̬��Ϣ����ʾ
		std::cout << std::endl << "JobID: " << job->GetSpec().job_id << "\tState: "<<ToString(info.job_state)<<"\tProgress: " << info.progress << std::endl;

		job->UpdateTaskInfo(tasks); // ����Task״̬��Ϣ�����뿪ʼ��ȡ����Task�б�
		if (tasks.empty())
			break; // ע�⣺UpdateTaskInfo�����ڲ����Ƴ���һ�θ���ʱ�Ѿ�������Task�����۳ɹ�����ʧ�ܣ�����˵��б�Ϊ�վͱ�ʾ���е�Task��������
		for (TaskInfo& t : tasks) {
			// ���Task��״̬��Ϣ
			std::cout << " ID=" << t.task_id << "\t\tName=" << t.task_name << "\tState=" << ToString(t.task_state) << "\tProgress=" << t.progress << "%\tNode="
				<< (t.exec_node.empty() ? "----" : t.exec_node) << "\tExitCode=" << t.exit_code;
			if (t.start_time && t.finish_time)
				std::cout << "\tDuration=" << (t.finish_time - t.start_time) << "s";
			std::cout << std::endl;
			if (t.IsFinished()) {
				// �����Ѿ������ˣ������׼��������
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
