#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <boost/scoped_ptr.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
#include <boost/bind.hpp>
#include "Falcon.h"
#include "HttpBase.h"
#include "SlaveServer.h"

namespace falcon {

const int HEARTBEAT_CHECK_INTERVAL = 1;

SlaveServer::SlaveServer() 
	: cpu_count(0), cpu_frequency(0), slave_addr("0.0.0.0"), slave_port(SLAVE_LISTEN_PORT), registered(false),
	  hb_interval(5), hb_elapsed(0), hb_counter(0)
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

	int max_try_count = 10, try_count = 0;
	while (!IsStopped()) {
		std::string request = reg_info.toStyledString();
		std::string result = HttpUtil::Post(master_addr + "/cluster/slaves", request);
		if (result.empty()) {
			LOG(ERROR) << "Failed to register salve server! Try again later.";
			std::this_thread::sleep_for(std::chrono::seconds((int)std::pow(2, try_count)));
			try_count++;
			if (try_count > max_try_count)
				break;
		} else {
			Json::Value response;
			if (!Util::ParseJsonFromString(result, response)) {
				LOG(ERROR) << "Invalid response from master server: " << result;
				return false;
			}
			if (response.isMember("error")) {
				LOG(ERROR) << "Failed to register in master server: " << response["error"].asString();
				return false;
			}
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

	// register in master server
	if (!RegisterSlave())
		return;

	// run event loop
	hb_timer.reset(new boost::asio::steady_timer(*ioctx, boost::asio::chrono::seconds(HEARTBEAT_CHECK_INTERVAL)));
	hb_timer->async_wait(boost::bind(&SlaveServer::Heartbeat, this, _1));
	ioctx->run();
	LOG(INFO) << "Slave server is going to shutdown";

	SlaveServer::Destory();
}

int SlaveServer::StopServer()
{
	is_stopped.store(true);
	if (listener)
		listener->Stop();
	return EXIT_SUCCESS;
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
	slave_resources.Set(RESOURCE_CPU, float(cpu_count));
	slave_resources.Set(RESOURCE_FREQ, cpu_count*cpu_frequency);
	slave_resources.Set(RESOURCE_MEM, Util::GetTotalMemory());

	std::string gpu_name;
	int num_gpus = 0, gpu_cores = 0;
	Util::GetGPUInfo(gpu_name, num_gpus, gpu_cores);
	slave_resources.Set(RESOURCE_GPU, num_gpus);
	// TODO Get the disk spaces
	slave_resources.Set(RESOURCE_DISK, 2048);
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
	  err_write_pipe(NULL), heartbeat(-1), exit_code(-1), exec_progress(0)
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
	Json::Value update(Json::arrayValue);
	do {
		std::unique_lock<std::mutex> lock_execs(exec_mutex);
		TaskExecInfoMap::iterator it = exec_tasks.begin();
		while (it != exec_tasks.end()) {
			TaskExecInfoPtr task = it->second;
			if (task->heartbeat == -1) {
				std::unique_lock<std::mutex> lock_task(task->mtx);
				task->heartbeat = hb_counter; // reset heartbeat flag
				// this task's status has changed
				Json::Value v(Json::objectValue);
				v["job_id"] = task->job_id;
				v["task_id"] = task->task_id;
				v["progress"] = task->exec_progress;
				v["tiptext"] = task->exec_tip;
				if (task->exit_code == -1) {
					v["state"] = ToString(Task::State::Executing);
					v["exit_code"] = -1;
					update.append(v);
				} else {
					// this task has finished
					if (task->exit_code == EXIT_SUCCESS)
						v["state"] = ToString(Task::State::Completed);
					else if(task->exit_code == EXIT_FAILURE)
						v["state"] = ToString(Task::State::Failed);
					else
						v["state"] = ToString(Task::State::Aborted);
					v["exit_code"] = task->exit_code;
					finished_tasks.insert(*it);
					it = exec_tasks.erase(it);
					update.append(v);
					continue;
				}
			}
			it++;
		}
		hb_message["load"] = int(exec_tasks.size());
		hb_message["update"] = update;
	} while (false);

	// check if heartbeat must be send
	if (update.size() == 0 && hb_elapsed < hb_interval)
		hb_elapsed++;  // do nothing, just increase elapsed counter
	else {
		// must send heartbeat information
		hb_elapsed = 0;
		std::string result = HttpUtil::Post(master_addr + "/cluster/heartbeats", hb_message.toStyledString());
		Json::Value response;
		if (!Util::ParseJsonFromString(result, response) || response.isMember("error")) {
			// TODO: save finished tasks into local file
		}
	}
	if (!IsStopped()) {
		hb_timer->expires_at(hb_timer->expiry() + boost::asio::chrono::seconds(HEARTBEAT_CHECK_INTERVAL));
		hb_timer->async_wait(boost::bind(&SlaveServer::Heartbeat, this, _1));
	}
}

static void StreamPipe2File(HANDLE pipe, std::ofstream& ofs)
{
	const int PIPE_READ_SIZE = 256;
	char buf[PIPE_READ_SIZE];
	while (true) {
		DWORD read_bytes;
		memset(buf, 0, PIPE_READ_SIZE);
		if (!ReadFile(pipe, buf, PIPE_READ_SIZE, &read_bytes, NULL))
			break;
		ofs.write(buf, read_bytes);
	}
}

void SlaveServer::MonitorTask(TaskExecInfoPtr task)
{
	// read std error output and save to error file
	std::thread err_thread([&task]() {
		char buf[257];
		while (true) {
			DWORD read_bytes;
			memset(buf, 0, 257);
			LOG(INFO) << "Read stderr pipe...";
			if (!ReadFile(task->err_read_pipe, buf, 256, &read_bytes, NULL))
				break;
			LOG(INFO) << "stderr:" << buf;
			task->err_file.write(buf, read_bytes);
		}
	});
	err_thread.detach();

	// read std out output, update progress and save to out file
	const int MAX_OUTPUT_LEN = 2048; // max output line length
	char line[MAX_OUTPUT_LEN] = { 0 };
	char* line_buf = line;
	bool in_endl = false;
	while (true) {
		char buf[257] = { 0 };
		DWORD read_bytes;
		LOG(INFO) << "Read stdout pipe...";
		if (!ReadFile(task->out_read_pipe, buf, 256, &read_bytes, NULL))
			break;
		LOG(INFO) << "stdout:" << buf;
		task->out_file.write(buf, read_bytes);
		//for (int i = 0; i < read_bytes; i++) {
		//	if (buf[i] == '\n' || buf[i] == '\r') {
		//		if (in_endl)
		//			continue;
		//		if (line[0] != '[') // example: [35%] creating output file
		//			task->out_file << line << std::endl;
		//		else {
		//			// parse progress value and tip string
		//			if (char* s = strchr(line, '%')) {
		//				*s = '\0';
		//				int progress = std::atoi(&line[1]);
		//				std::string tip = s + 3; // skip ']' and blackspace, could be empty
		//				std::unique_lock<std::mutex> lock(task->mtx);
		//				if (task->exec_progress != progress || (!tip.empty() && task->exec_tip != tip)) {
		//					task->exec_progress = progress;
		//					task->exec_tip = tip;
		//					task->heartbeat = -1;
		//				}
		//			}
		//		}
		//		memset(line, 0, MAX_OUTPUT_LEN);
		//		line_buf = line;
		//		in_endl = true;
		//	} else {
		//		*line_buf = buf[i];
		//		line_buf++;
		//		in_endl = false;
		//	}
		//}
	}

	DWORD exit_code = EXIT_SUCCESS;
	WaitForSingleObject(task->process_info.hProcess, INFINITE);
	if (GetExitCodeProcess(task->process_info.hProcess, &exit_code) == FALSE)
		LOG(ERROR) << "Failed to get process exit code: " << Util::GetLastErrorMessage();
	else
		LOG(INFO) << "Task " << task->job_id << "." << task->task_id << " is exited with code " << exit_code;

	do {
		std::unique_lock<std::mutex> lock(task->mtx);
		task->heartbeat = -1;
		task->exit_code = (int)exit_code;
		task->out_file.close();
		task->err_file.close();
		CloseHandle(task->out_read_pipe);
		task->out_read_pipe = NULL;
		CloseHandle(task->err_read_pipe);
		task->err_read_pipe = NULL;
	} while (false);

	// transfer stdout and stderr files to cluster master
	if (boost::filesystem::file_size(task->out_file_path) != 0) {

	}
	if (boost::filesystem::file_size(task->err_file_path) != 0) {

	}
}

void SlaveServer::AddExecutingTask(TaskExecInfoPtr task)
{
	std::unique_lock<std::mutex> lock(exec_mutex);
	exec_tasks.insert(std::make_pair(task->job_id + "." + task->task_id, task));
	std::thread task_thread(boost::bind(&SlaveServer::MonitorTask, this, task));
	task_thread.detach();
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
			return "Illegal json body for registering slave";
		LOG(INFO) << "New task(" << value["job_id"].asString() << "." << value["task_id"].asString() << ") received from master " << remote;

		TaskPtr task(new Task(value["job_id"].asString(), value["task_id"].asString(), value["content"]["name"].asString()));
		task->Assign(value["content"], nullptr);

		Json::Value response(Json::objectValue);
		response["state"] = ToString(Task::State::Executing);
		response["time"] = time(NULL);
		response["machine"] = server->GetHostName();

		TaskExecInfoPtr exec_info(new TaskExecInfo());
		exec_info->job_id  = task->job_id;
		exec_info->task_id = task->task_id;
		exec_info->local_dir = boost::str(boost::format("%s/%s/%s") % server->GetTaskDir() % task->job_id % task->task_id);
		LOG(INFO) << "Create task local directory " << exec_info->local_dir;
		if (!boost::filesystem::create_directories(exec_info->local_dir)) {
			response["state"] = ToString(Task::State::Aborted);
			response["message"] = "Failed to create task local directory " + exec_info->local_dir;
			LOG(ERROR) << response["message"].asString();
			return response.toStyledString();
		}
		LOG(INFO) << "Create stdout/stderr file for new task";
		exec_info->out_file_path = boost::str(boost::format("%s/%s.out") % exec_info->local_dir % task->task_id);
		exec_info->out_file.open(exec_info->out_file_path, std::ios_base::out);
		exec_info->err_file_path = boost::str(boost::format("%s/%s.err") % exec_info->local_dir % task->task_id);
		exec_info->err_file.open(exec_info->err_file_path, std::ios_base::out);
		if (!exec_info->out_file.is_open() || !exec_info->err_file) {
			response["state"] = ToString(Task::State::Aborted);
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
			std::string errmsg = Util::GetLastErrorMessage();
			LOG(ERROR) << "Failed to create pipe: " << errmsg;
			response["state"] = ToString(Task::State::Aborted);
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

		TCHAR** envs_block = NULL;
		if (!task->exec_envs.empty()) {
			LOG(INFO) << "Build environment variables block for " << task->exec_envs;
			std::vector<std::string> env_strs;
			boost::split(env_strs, task->exec_envs, boost::is_any_of(";"));
			envs_block = new TCHAR*[env_strs.size()+1];
			for (size_t i = 0; i < env_strs.size(); i++) {
				envs_block[i] = new TCHAR[env_strs[i].length() + 1];
				memset(envs_block[i], 0, sizeof(TCHAR)*(env_strs[i].length() + 1));
				strcpy(envs_block[i], env_strs[i].c_str());
			}
			envs_block[env_strs.size()] = NULL;
		}

		LOG(INFO) << "Startup new process for command " << task->exec_command;
		/*if (CreateProcess(task->exec_command.c_str(), args.get(), NULL, NULL, TRUE, NULL, (LPVOID)envs_block,
			task->work_dir.empty() ? NULL : task->work_dir.c_str(), &startupinfo, &exec_info->process_info) == FALSE) {*/
		if (CreateProcess("C:\\Temp\\BuildModel\\x64\\Release\\BuildModel.exe", NULL, NULL, NULL, TRUE, NULL, NULL, NULL, &startupinfo, &exec_info->process_info) == FALSE) {
			response["message"] = Util::GetLastErrorMessage();
			response["state"] = ToString(Task::State::Aborted);
		}
		CloseHandle(exec_info->out_write_pipe);
		exec_info->out_write_pipe = NULL;
		CloseHandle(exec_info->err_write_pipe);
		exec_info->err_write_pipe = NULL;

		if (envs_block) {
			for (int i = 0; envs_block[i] != NULL; i++)
				delete[] envs_block[i];
			delete[] envs_block;
		}
		if (FromString<Task::State>(response["state"].asCString()) == Task::State::Aborted) {
			LOG(ERROR) << "Start process failed: " << response["message"].asString();
			return response.toStyledString();
		}
		LOG(INFO) << "Process is started: " << exec_info->process_info.dwProcessId;
		exec_info->startup_time = time(NULL);

		server->AddExecutingTask(exec_info);
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
