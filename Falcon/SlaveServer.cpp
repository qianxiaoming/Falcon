﻿#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <boost/scoped_ptr.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/bind.hpp>
#include <winsock2.h>
#include <Windows.h>
#include <Psapi.h>
#include <LM.h>
#include "Falcon.h"
#include "HttpBase.h"
#include "SlaveServer.h"

namespace falcon {

const int HEARTBEAT_CHECK_INTERVAL = 1;

SlaveServer::SlaveServer() 
	: cpu_count(0), cpu_frequency(0), slave_addr("0.0.0.0"), slave_port(SLAVE_LISTEN_PORT), registered(false),
	  hb_interval(5), hb_elapsed(0), hb_counter(0), hb_error(0)
{
}

bool SlaveServer::StartServer()
{
	// get machine information
	if (!CollectSystemInfo())
		return false;

	SlaveServer* server = SlaveServer::Instance();
	server->SetupAPIHandler();
	if (!server->SetupListenHTTP())
		return false;

	return true;
}

bool MountRemoteFileSys(bool& has_config)
{
	has_config = false;
	// Z=cifs:\\192.168.5.201\data
	std::string config_file = falcon::Util::GetModulePath() + "/Wit3d-Slave.conf";
	if (!boost::filesystem::exists(config_file)) {
		LOG(ERROR) << "Slave configuration file not found. No remote file system to be mount";
		return true;
	}

	FILE* f = fopen(config_file.c_str(), "r");
	if (!f) {
		LOG(ERROR) << "Cannot open slave config file " << config_file;
		return false;
	}
	std::string remote_filesys;
	while (!feof(f)) {
		char line[256] = { 0 };
		if (!fgets(line, 255, f))
			break;
		if (!boost::starts_with(line, "mount"))
			continue;
		fclose(f);
		char* eq = strchr(line, '=');
		if (eq) {
			remote_filesys = eq + 1;
			boost::trim(remote_filesys);
			break;
		} else {
			LOG(ERROR) << "Invalid mount configuration: " << line;
			return false;
		}
	}
	if (remote_filesys.empty()) {
		LOG(INFO) << "No remote file system configurated, skip mount operation";
		return true;
	}

	LOG(INFO) << "Mount remote file system as " << remote_filesys;
	std::string local_disk_name, remote_share_add;
	std::string::size_type pos = remote_filesys.find('=');
	if (pos != std::string::npos) {
		local_disk_name = boost::trim_copy(remote_filesys.substr(0, pos));
		std::string val = boost::trim_copy(remote_filesys.substr(pos + 1));
		pos = val.find(':');
		if (pos != std::string::npos)
			remote_share_add = boost::trim_copy(val.substr(pos + 1));
	}
	if (local_disk_name.empty() || remote_share_add.empty()) {
		LOG(ERROR) << "Invalid format for mount path configuration";
		return false;
	}

	has_config = true;
	if (!boost::ends_with(local_disk_name, ":"))
		local_disk_name = boost::str(boost::format("%s:") % local_disk_name);

	// NetUseAdd/NetUseDel
	int length = MultiByteToWideChar(CP_ACP, MB_PRECOMPOSED, local_disk_name.c_str(), -1, NULL, 0);
	if (length == 0)
		return false;
	boost::scoped_array<wchar_t> local_result(new wchar_t[length]);
	MultiByteToWideChar(CP_ACP, MB_PRECOMPOSED, local_disk_name.c_str(), -1, local_result.get(), length);
	LOG(INFO) << "  Local disk name: " << local_disk_name;

	length = MultiByteToWideChar(CP_ACP, MB_PRECOMPOSED, remote_share_add.c_str(), -1, NULL, 0);
	if (length == 0)
		return false;
	boost::scoped_array<wchar_t> remote_result(new wchar_t[length]);
	MultiByteToWideChar(CP_ACP, MB_PRECOMPOSED, remote_share_add.c_str(), -1, remote_result.get(), length);
	LOG(INFO) << "  Remote share path: " << remote_share_add;

	USE_INFO_2 use_info;
	memset(&use_info, '\0', sizeof use_info);
	use_info.ui2_local = local_result.get();
	use_info.ui2_remote = remote_result.get();

	DWORD ret = 0;
	ret = NetUseAdd(NULL, 2, (BYTE *)&use_info, NULL);
	if (ret != NERR_Success) {
		if (ret == ERROR_ACCESS_DENIED)
			LOG(ERROR) << "Failed to mount remote path to local disk: Access is denied.";
		else
			LOG(ERROR) << "Mount remote path failed with " << ret << ": " << Util::GetErrorMessage(ret);
		return false;
	}
	LOG(INFO) << "Successfully mounted " << remote_filesys;
	return true;
}

bool SlaveServer::RegisterSlave()
{
	LOG(INFO) << "Registering in master server " << master_addr << "...";
	registered = false;
	Json::Value reg_info(Json::objectValue), res_info(Json::objectValue);
	reg_info["name"]      = slave_name;
	reg_info["port"]      = slave_port;
	reg_info["os"]        = os_name;
	reg_info["version"]   = os_version;

	Json::Value cpu_info(Json::objectValue);
	cpu_info["count"]     = cpu_count;
	cpu_info["frequency"] = cpu_frequency;
	reg_info["cpu"]       = cpu_info;

	reg_info["resources"] = slave_resources.ToJson();

	int try_count = 0;
	std::string request = reg_info.toStyledString();
	while (!IsStopped()) {
		std::string result = HttpUtil::Post(master_addr + "/cluster/slaves", request);
		Json::Value response;
		if (!Util::ParseJsonFromString(result, response)) {
			LOG(ERROR) << "Invalid response from master server: " << result;
			return false;
		}
		if (response.isMember("error")) {
			LOG(ERROR) << "Failed to register in master server: " << response["error"].asString();
			try_count++;
			if ((try_count % 20) == 0)
				LOG(INFO) << "Already try " << try_count << " times to register slave server";
			std::this_thread::sleep_for(std::chrono::seconds(5));
			continue;
		} else {
			cluster_name = response["cluster"].asString();
			slave_id     = response["id"].asString();
			slave_addr   = response["addr"].asString();
			hb_interval  = response["heartbeat"].asInt();
			registered   = true;
			LOG(INFO) << "Slave("<<slave_id<<") registered in cluster " << cluster_name << " and heartbeat interval is " << hb_interval << " seconds";
			return true;
		}
	}
	return false;
}

void SlaveServer::RunServer()
{
	SlaveServer* server = SlaveServer::Instance();
	LOG(INFO) << "Slave server is running...";
	
	int pid = (int)GetCurrentProcessId();
	std::string pid_file_name = boost::str(boost::format("slave-%d.pid") % slave_port);
	FILE* fpid = fopen(pid_file_name.c_str(), "w");
	if (fpid) {
		char pid_str[64] = { 0 };
		sprintf(pid_str, "%d", pid);
		fwrite(pid_str, 1, strlen(pid_str), fpid);
		fclose(fpid);
	}
	else
		LOG(ERROR) << "Can not create pid file for slave service";

	bool has_config;
	if (!MountRemoteFileSys(has_config)) {
		if (has_config) {
			auto const mount_thread_func = []()
			{
				bool has_config = true, mounted = false;
				while (!mounted && has_config) {
					std::this_thread::sleep_for(std::chrono::seconds(10));
					mounted = MountRemoteFileSys(has_config);
				}
			};
			std::thread mount_thread(mount_thread_func);
			mount_thread.detach();
		}
	}

	// register in master server
	if (!RegisterSlave())
		return;

	// run event loop
	LOG(INFO) << "Start event loop of slave service with heartbeat " << HEARTBEAT_CHECK_INTERVAL << "s...";
	hb_timer.reset(new boost::asio::steady_timer(*ioctx, boost::asio::chrono::seconds(HEARTBEAT_CHECK_INTERVAL)));
	hb_timer->async_wait(boost::bind(&SlaveServer::Heartbeat, this, _1));
	ioctx->run();
	LOG(INFO) << "Slave server is going to shutdown";

	SlaveServer::Destory();
}

int SlaveServer::StopServer()
{
	is_stopped.store(true);

	if (ClearTasks()) {
		std::this_thread::sleep_for(std::chrono::seconds(HEARTBEAT_CHECK_INTERVAL * 2));
		boost::system::error_code ec;
		Heartbeat(ec);
	}

	if (listener)
		listener->Stop();

	std::string pid_file_name = boost::str(boost::format("slave-%d.pid") % slave_port);
	boost::filesystem::remove(pid_file_name);

	return EXIT_SUCCESS;
}

int SlaveServer::ClearTasks()
{
	if (exec_tasks.empty())
		return 0;
	
	LOG(INFO) << "Terminating executing tasks...";
	std::unique_lock<std::mutex> lock(exec_mutex);
	int count = 0;
	TaskExecInfoMap::iterator it = exec_tasks.begin();
	for (; it != exec_tasks.end(); it++) {
		if (!TerminateProcess(it->second->process_info.hProcess, ERROR_PROCESS_RESCHEDULE)) {
			LOG(WARNING) << "Unable to terminate process for task(" << it->first << ":" << Util::GetLastErrorMessage(NULL);
		}
		else
			count++;
	}
	LOG(INFO) << count << " processes are terminated";
	return count;
}

const char* SlaveServer::GetName()
{
	return FALCON_SLAVE_SERVER_NAME;
}

static SlaveServer* slave_instance = nullptr;

SlaveServer* SlaveServer::Instance()
{
	if (slave_instance == nullptr)
		slave_instance = new SlaveServer;
	return slave_instance;
}

void SlaveServer::Destory()
{
	delete slave_instance;
	slave_instance = nullptr;
}

void SlaveServer::SetMasterAddr(std::string addr)
{
	master_addr = addr;
	if (master_addr.find(':') == std::string::npos)
		master_addr += boost::str(boost::format(":%d") % MASTER_SLAVE_PORT);
}

void SlaveServer::SetSlavePort(uint16_t port)
{
	slave_port = port;
}

bool SlaveServer::CollectSystemInfo()
{
	char name[256] = { 0 };
	if (gethostname(name, sizeof(name)) != 0) {
		PLOG(ERROR) << "Unable to get host name";
		return false;
	}
	slave_name = name;
	os_name = "Windows"; // just for now
	os_version = "";
	task_path = Util::GetModulePath() + "/tasks";

	std::string proc_name;
	Util::GetCPUInfo(proc_name, cpu_count, cpu_frequency);
	if (cpu_count == 0 || cpu_frequency == 0)
		return false;
	//cpu_count = 12;
	slave_resources.Set(RESOURCE_CPU, float(cpu_count));
	slave_resources.Set(RESOURCE_FREQ, cpu_count*cpu_frequency);
	slave_resources.Set(RESOURCE_MEM, Util::GetTotalMemory());

	std::string gpu_name;
	int num_gpus = 0, gpu_cores = 0;
	Util::GetGPUInfo(gpu_name, num_gpus, gpu_cores);
	slave_resources.Set(RESOURCE_GPU, num_gpus);
	// TODO Get the disk spaces
	slave_resources.Set(RESOURCE_DISK, 2048*1024);
	return true;
}

bool SlaveServer::SetupListenHTTP()
{
	LOG(INFO) << "Setup HTTP service for cluster master on " << slave_addr << ":" << slave_port << "...";
	auto const address = boost::asio::ip::make_address(slave_addr);

	ioctx = boost::make_shared<boost::asio::io_context>();
	boost::asio::io_context& ioc = *ioctx;

	listener = std::make_shared<Listener>(
		ioc,
		tcp::endpoint{ address, slave_port },
		boost::bind(&SlaveServer::HandleMasterRequest, this, _1, _2, _3, _4, _5));
	if (!listener->IsListening()) {
		listener.reset();
		return false;
	}
	listener->Accept();
	LOG(INFO) << "HTTP service for cluster master OK";
	return true;
}

TaskExecInfo::TaskExecInfo()
	: startup_time(0), out_read_pipe(NULL), out_write_pipe(NULL), err_read_pipe(NULL),
	  err_write_pipe(NULL), heartbeat(-1), is_executing(true), exit_code(0), exec_progress(0)
{
	ZeroMemory(&process_info, sizeof(PROCESS_INFORMATION));
}

TaskExecInfo::~TaskExecInfo()
{
	CloseHandle(out_read_pipe);
	CloseHandle(out_write_pipe);
	CloseHandle(err_read_pipe);
	CloseHandle(err_write_pipe);
	CloseHandle(process_info.hThread);
	CloseHandle(process_info.hProcess);
}

struct SlaveAPI
{
	Handler<SlaveServer>* FindHandler(const std::string& target)
	{
		for (auto& v : handlers) {
			if (v.first == target)
				return v.second;
		}
		return nullptr;
	}
	void RegisterHandler(const std::string& target, Handler<SlaveServer>* handler)
	{
		handlers.push_back(std::make_pair(target, handler));
	}
	std::list<std::pair<std::string, Handler<SlaveServer>*>> handlers;
};
static SlaveAPI slave_api;

std::string SlaveServer::HandleMasterRequest(
	const std::string& remote_addr,
	http::verb verb,
	const std::string& target,
	const std::string& body,
	http::status& status)
{
	URLParamMap params;
	std::string api_target = HttpUtil::ParseHttpURL(target, 0, params);

	Handler<SlaveServer>* handler = slave_api.FindHandler(api_target);
	if (handler) {
		if (verb == http::verb::get)
			return handler->Get(this, remote_addr, api_target, params, status);
		else if (verb == http::verb::post)
			return handler->Post(this, remote_addr, api_target, params, body, status);
		else if (verb == http::verb::delete_)
			return handler->Delete(this, remote_addr, api_target, params, body, status);
		else {
			status = http::status::bad_request;
			return "Unsupported HTTP-method";
		}
	}

	status = http::status::bad_request;
	return "Illegal request-target";
}

// send heartbeat or task update information to master server
void SlaveServer::Heartbeat(const boost::system::error_code& e)
{
	hb_counter++;
	// check for task update information and send as heartbeat if available
	Json::Value hb_message(Json::objectValue);
	hb_message["id"] = slave_id;
	TaskExecInfoMap finished_tasks;
	std::list<TaskExecInfoPtr> reset_tasks;
	Json::Value updates(Json::arrayValue);
	do {
		std::unique_lock<std::mutex> lock_execs(exec_mutex);
		TaskExecInfoMap::iterator it = exec_tasks.begin();
		while (it != exec_tasks.end()) {
			TaskExecInfoPtr task = it->second;
			if (task->heartbeat == -1) {
				std::unique_lock<std::mutex> lock_task(task->mtx);
				reset_tasks.push_back(task);
				// this task's status has changed
				Json::Value v(Json::objectValue);
				v["job_id"] = task->job_id;
				v["task_id"] = task->task_id;
				v["progress"] = task->exec_progress;
				v["tiptext"] = task->exec_tip;
				if (task->is_executing) {
					v["state"] = ToString(TaskState::Executing);
					updates.append(v);
				} else {
					// this task has finished
					if (task->exit_code == EXIT_SUCCESS)
						v["state"] = ToString(TaskState::Completed);
					else if(task->exit_code == EXIT_FAILURE)
						v["state"] = ToString(TaskState::Failed);
					else if(task->exit_code == ERROR_PROCESS_ABORTED)
						v["state"] = ToString(TaskState::Terminated);
					else
						v["state"] = ToString(TaskState::Aborted);
					v["exit_code"] = task->exit_code;
					v["error_msg"] = task->error_msg;
					finished_tasks.insert(*it);
					it = exec_tasks.erase(it);
					updates.append(v);
					continue;
				}
			}
			it++;
		}
		hb_message["load"] = int(exec_tasks.size());
		hb_message["updates"] = updates;
	} while (false);

	// check if heartbeat must be send
	bool is_stopped = IsStopped();
	if (updates.size() == 0 && hb_elapsed < hb_interval)
		hb_elapsed++;  // do nothing, just increase elapsed counter
	else {
		// must send heartbeat information
		DLOG(INFO) << "Sending heartbeat to master " << master_addr << " with " << hb_message["updates"].size() << " updates...";
		hb_elapsed = 0;
		std::string result = HttpUtil::Post(master_addr + "/cluster/heartbeats", hb_message.toStyledString());
		Json::Value response;
		if (!Util::ParseJsonFromString(result, response) || response.isMember("error")) {
			LOG(WARNING) << "Failed to send heartbeat to master " << master_addr << ": " << (response.isNull() ? result : Util::JsonToString(response));
			hb_error++;
			if (hb_error >= 3 && is_stopped == false) {
				RegisterSlave();
				ClearTasks();
				exec_tasks.clear();
				hb_error = 0;
			}
		} else {
			for (auto& task : reset_tasks)
				task->heartbeat = hb_counter; // reset heartbeat flag
			hb_error = 0;
		}
	}
	if (!is_stopped) {
		hb_timer->expires_at(hb_timer->expiry() + boost::asio::chrono::seconds(HEARTBEAT_CHECK_INTERVAL));
		hb_timer->async_wait(boost::bind(&SlaveServer::Heartbeat, this, _1));
	}
}

void SlaveServer::MonitorTask(TaskExecInfoPtr task)
{
	// read std error output and save to error file
	std::thread err_thread([&task]() {
		const int BUFFER_LEN = 2048;
		char buf[BUFFER_LEN] = { 0 };
		while (true) {
			DWORD read_bytes = 0;
			memset(buf, 0, BUFFER_LEN);
			if (!::ReadFile(task->err_read_pipe, buf, BUFFER_LEN-1, &read_bytes, NULL))
				break;
			if (read_bytes > 0) {
				char *cur = buf, *pos = buf;
				for (DWORD i = 0; i < read_bytes; i++, cur++) {
					*pos = *cur;
					if (*pos != '\r')
						pos++;
				}
				*pos = '\0';
				read_bytes = DWORD(pos - buf);
			}
			task->err_file.write(buf, read_bytes);
			task->err_file.flush();
		}
	});
	err_thread.detach();

	// read std out output, update progress and save to out file
	const int MAX_OUTPUT_LEN = 2048; // max output line length
	char line[MAX_OUTPUT_LEN] = { 0 };
	char* line_buf = line;
	bool in_endl = false;
	while (true) {
		char buf[256] = { 0 };
		DWORD read_bytes = 0;
		if (!::ReadFile(task->out_read_pipe, buf, 256, &read_bytes, NULL))
			break;
		for (size_t i = 0; i < read_bytes; i++) {
			if (buf[i] == '\n' || buf[i] == '\r') {
				if (in_endl)
					continue;
				if (line[0] != '[') // example: [35%] creating output file
					task->out_file << line << std::endl;
				else {
					// parse progress value and tip string
					if (char* s = strchr(line, '%')) {
						*s = '\0';
						int progress = std::atoi(&line[1]);
						std::string tip = s + 3; // skip ']' and blackspace, could be empty
						std::unique_lock<std::mutex> lock(task->mtx);
						if (task->exec_progress != progress || (!tip.empty() && task->exec_tip != tip)) {
							task->exec_progress = progress;
							task->exec_tip = tip;
							task->heartbeat = -1;
						}
					}
				}
				memset(line, 0, MAX_OUTPUT_LEN);
				line_buf = line;
				in_endl = true;
			} else {
				*line_buf = buf[i];
				line_buf++;
				in_endl = false;
			}
		}
	}

	DWORD exit_code = EXIT_SUCCESS;
	WaitForSingleObject(task->process_info.hProcess, INFINITE);
	if (GetExitCodeProcess(task->process_info.hProcess, &exit_code) == FALSE)
		LOG(ERROR) << "Failed to get process exit code: " << Util::GetLastErrorMessage(NULL);
	else
		LOG(INFO) << "Task " << task->job_id << "." << task->task_id << " is exited with code " << exit_code;

	do {
		std::unique_lock<std::mutex> lock(task->mtx);
		task->is_executing = false;
		task->heartbeat = -1;
		task->exit_code = exit_code;
		if (exit_code & 0xC0000000) {
			task->error_msg = "Abort by unhandled exception";
			if (exit_code == 0xC0000005)
				task->error_msg += ": Access violation";
		} else if (exit_code == ERROR_PROCESS_ABORTED)
			task->error_msg = "Terminated by user";
		else if (exit_code == ERROR_PROCESS_RESCHEDULE)
			task->error_msg = "Expelled from executing node";
		task->out_file.close();
		task->err_file.close();
		CloseHandle(task->out_read_pipe);
		task->out_read_pipe = NULL;
		CloseHandle(task->err_read_pipe);
		task->err_read_pipe = NULL;
	} while (false);

	if (exit_code != ERROR_PROCESS_ABORTED && exit_code != ERROR_PROCESS_RESCHEDULE) {
		// transfer stdout and stderr files to cluster master
		auto UploadTaskLogFile = [&task](const std::string& file_path, const std::string& master_addr) {
			if (!boost::filesystem::exists(file_path))
				return;
			uintmax_t out_size = boost::filesystem::file_size(file_path);
			if (out_size != 0) {
				LOG(INFO) << "Upload log file for task " << task->job_id << "." << task->task_id << ": " << file_path;
				std::string file_name = boost::filesystem::path(file_path).filename().string();
				std::string url = boost::str(boost::format("%s/cluster/logs?name=%s&job_id=%s&task_id=%s")
					% master_addr % file_name % task->job_id % task->task_id);
				if (!HttpUtil::UploadFile(url, file_path, out_size))
					LOG(ERROR) << "Failed to update task log file " << file_path;
			}
		};
		UploadTaskLogFile(task->out_file_path, master_addr);
		UploadTaskLogFile(task->err_file_path, master_addr);
	}
}

void SlaveServer::AddExecutingTask(TaskExecInfoPtr task)
{
	std::unique_lock<std::mutex> lock(exec_mutex);
	exec_tasks.insert(std::make_pair(task->job_id + "." + task->task_id, task));
	std::thread task_thread(boost::bind(&SlaveServer::MonitorTask, this, task));
	task_thread.detach();
}

bool SlaveServer::TerminateTask(const std::string& job_id, const std::string& task_id, std::string& errmsg)
{
	std::unique_lock<std::mutex> lock(exec_mutex);
	TaskExecInfoMap::iterator it = exec_tasks.find(job_id + "." + task_id);
	if (it == exec_tasks.end()) {
		errmsg = "task not found";
		return false;
	}
	if (!TerminateProcess(it->second->process_info.hProcess, ERROR_PROCESS_ABORTED)) {
		errmsg = Util::GetLastErrorMessage(NULL);
		return false;
	}
	return true;
}

namespace handler {
namespace slave {

// handler for "/tasks" endpoint
struct TasksHandler : public Handler<SlaveServer>
{
	// get new task from master
	virtual std::string Post(SlaveServer* server, const std::string& remote, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		Json::Value value;
		if (!Util::ParseJsonFromString(body, value))
			return "Illegal json body for executing task";
		LOG(INFO) << "New task(" << value["job_id"].asString() << "." << value["task_id"].asString() << ") received from master " << remote;

		TaskPtr task(new Task(value["job_id"].asString(), value["task_id"].asString(), value["content"]["name"].asString()));
		task->Assign(value["content"], nullptr);

		Json::Value response(Json::objectValue);
		response["state"] = ToString(TaskState::Executing);
		response["time"] = time(NULL);
		response["machine"] = server->GetHostName();

		TaskExecInfoPtr exec_info(new TaskExecInfo());
		exec_info->job_id  = task->job_id;
		exec_info->task_id = task->task_id;
		exec_info->local_dir = boost::str(boost::format("%s/%s/%s") % server->GetTaskDir() % task->job_id % task->task_id);
		if (!boost::filesystem::exists(exec_info->local_dir)) {
			LOG(INFO) << "Create task local directory " << exec_info->local_dir;
			if (!boost::filesystem::create_directories(exec_info->local_dir)) {
				response["state"] = ToString(TaskState::Aborted);
				response["exit_code"] = EXIT_FAILURE;
				response["message"] = "Failed to create task local directory " + exec_info->local_dir;
				LOG(ERROR) << response["message"].asString();
				return response.toStyledString();
			}
		}
		LOG(INFO) << "Create stdout/stderr file for new task";
		exec_info->out_file_path = boost::str(boost::format("%s/%s.out") % exec_info->local_dir % task->task_id);
		exec_info->out_file.open(exec_info->out_file_path, std::ios_base::out|std::ios_base::app);
		exec_info->err_file_path = boost::str(boost::format("%s/%s.err") % exec_info->local_dir % task->task_id);
		exec_info->err_file.open(exec_info->err_file_path, std::ios_base::out|std::ios_base::app);
		if (!exec_info->out_file.is_open() || !exec_info->err_file) {
			response["state"] = ToString(TaskState::Aborted);
			response["exit_code"] = EXIT_FAILURE;
			response["message"] = "Failed to create task stdout/stderr file under task local directory " + exec_info->local_dir;
			LOG(ERROR) << response["message"].asString();
			return response.toStyledString();
		}

		SECURITY_ATTRIBUTES stdoutsec;
		stdoutsec.nLength = sizeof(SECURITY_ATTRIBUTES);
		stdoutsec.bInheritHandle = true;
		stdoutsec.lpSecurityDescriptor = NULL;

		SECURITY_ATTRIBUTES stderrsec;
		stderrsec.nLength = sizeof(SECURITY_ATTRIBUTES);
		stderrsec.bInheritHandle = true;
		stderrsec.lpSecurityDescriptor = NULL;

		LOG(INFO) << "Create stdout/stderr pipe for new task process";
		if (!CreatePipe(&exec_info->out_read_pipe, &exec_info->out_write_pipe, &stdoutsec, NULL) ||
			!CreatePipe(&exec_info->err_read_pipe, &exec_info->err_write_pipe, &stderrsec, NULL)) {
			int code = 0;
			std::string errmsg = Util::GetLastErrorMessage(&code);
			LOG(ERROR) << "Failed to create pipe: " << errmsg;
			response["state"] = ToString(TaskState::Aborted);
			response["exit_code"] = code;
			response["message"] = errmsg;
			return response.toStyledString();
		}

		STARTUPINFO startupinfo;
		ZeroMemory(&startupinfo, sizeof(STARTUPINFO));
		startupinfo.cb = sizeof(STARTUPINFO);
		GetStartupInfo(&startupinfo);
		startupinfo.hStdError = exec_info->err_write_pipe;
		startupinfo.hStdOutput = exec_info->out_write_pipe;
		startupinfo.dwFlags = STARTF_USESHOWWINDOW | STARTF_USESTDHANDLES;
		startupinfo.wShowWindow = SW_HIDE;

		boost::scoped_ptr<TCHAR> args;
		if (!task->exec_args.empty()) {
			args.reset(new TCHAR[task->exec_args.length() + 1]);
			memset(args.get(), 0, sizeof(TCHAR)*(task->exec_args.length() + 1));
			strcpy(args.get(), task->exec_args.c_str());
			LOG(INFO) << "Task arguments: " << args.get();
		}

		boost::scoped_array<TCHAR> envs_block;
		if (!task->exec_envs.empty()) {
			LOG(INFO) << "Build environment variables block for " << task->exec_envs;
			size_t buf_len = task->exec_envs.length() + 2;
			envs_block.reset(new TCHAR[buf_len]);
			memset(envs_block.get(), 0, sizeof(TCHAR)*buf_len);
			strcpy(envs_block.get(), task->exec_envs.c_str());
			for (size_t i = 0; i < buf_len; i++)
				if (envs_block[i] == ';') envs_block[i] = '\0';
		}

		LOG(INFO) << "Startup new process for command " << task->exec_command;
		if (CreateProcess(task->exec_command.c_str(), args.get(), NULL, NULL, TRUE, NULL, (LPVOID)envs_block.get(),
			task->work_dir.empty() ? NULL : task->work_dir.c_str(), &startupinfo, &exec_info->process_info) == FALSE) {
			response["state"] = ToString(TaskState::Aborted);
			int code = 0;
			response["message"] = Util::GetLastErrorMessage(&code);
			response["exit_code"] = code;
		}
		CloseHandle(exec_info->out_write_pipe);
		exec_info->out_write_pipe = NULL;
		CloseHandle(exec_info->err_write_pipe);
		exec_info->err_write_pipe = NULL;

		if (ToTaskState(response["state"].asCString()) == TaskState::Aborted) {
			LOG(ERROR) << "Start process failed: " << response["message"].asString();
			return response.toStyledString();
		}
		LOG(INFO) << "Process is started: " << exec_info->process_info.dwProcessId;
		exec_info->startup_time = time(NULL);

		server->AddExecutingTask(exec_info);
		return response.toStyledString();
	}

	virtual std::string Delete(SlaveServer* server, const std::string& remote, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		Json::Value value;
		if (!Util::ParseJsonFromString(body, value))
			return "Illegal json body for terminating task";
		std::string job_id = value["job_id"].asString(), task_id = value["task_id"].asString();
		LOG(INFO) << "Request for terminating task(" << job_id << "." << task_id << ") is received from master " << remote;

		Json::Value response(Json::objectValue);
		std::string errmsg;
		if (server->TerminateTask(job_id, task_id, errmsg))
			response["status"] = "ok";
		else
			response["error"] = errmsg;
		return response.toStyledString();
	}
};

}
}

void SlaveServer::SetupAPIHandler()
{
	slave_api.RegisterHandler("/tasks", new handler::slave::TasksHandler());
}

}
