﻿#ifndef FALCON_SLAVE_SERVER_H
#define FALCON_SLAVE_SERVER_H

#include <string>
#include <list>
#include <fstream>
#include <boost/smart_ptr.hpp>
#include <boost/asio/io_context.hpp>
#include "blockingconcurrentqueue.h"
#include "HttpBase.h"
#include "ServerBase.h"
#include "Util.h"

namespace falcon {

typedef boost::scoped_ptr<boost::asio::steady_timer> HeartbeatTimerPtr;

#ifdef WIN32
struct TaskExecInfo
{
	TaskExecInfo();
	~TaskExecInfo();

	std::string job_id;
	std::string task_id;
	time_t startup_time;
	PROCESS_INFORMATION process_info;
	HANDLE out_read_pipe;
	HANDLE out_write_pipe;
	HANDLE err_read_pipe;
	HANDLE err_write_pipe;
	std::string local_dir;
	std::string out_file_path;
	std::string err_file_path;
	std::ofstream out_file;
	std::ofstream err_file;

	std::mutex mtx;
	int heartbeat;
	bool is_executing;
	uint32_t exit_code;
	std::string error_msg;
	int exec_progress;
	std::string exec_tip;
};
#endif
typedef std::shared_ptr<TaskExecInfo> TaskExecInfoPtr;
typedef std::map<std::string, TaskExecInfoPtr> TaskExecInfoMap;

// Slave server is responsible for executing task dispatched from master server
class SlaveServer : public ServerBase
{
public:
	SlaveServer();

	static SlaveServer* Instance();

	static void Destory();

	virtual bool StartServer();

	virtual void RunServer();

	virtual int StopServer();

	virtual const char* GetName();

public:
	void SetMasterAddr(std::string addr);

	void SetSlavePort(uint16_t port);

	bool SetupListenHTTP();

	std::string GetHostName() const { return slave_name; }

	std::string GetTaskDir() const { return task_path; }

	void AddExecutingTask(TaskExecInfoPtr task);

	bool TerminateTask(const std::string& job_id, const std::string& task_id, std::string& errmsg);

	std::string HandleMasterRequest(
		const std::string& remote_addr,
		http::verb verb,
		const std::string& target,
		const std::string& body,
		http::status& status);

private:
	void SetupAPIHandler();

	bool CollectSystemInfo();

	bool RegisterSlave();

	void Heartbeat(const boost::system::error_code&);

	void MonitorTask(TaskExecInfoPtr task);

	int  ClearTasks();

	std::string               cluster_name;
	std::string               slave_name;
	std::string               slave_id;
	std::string               os_name;
	std::string               os_version;
	int                       cpu_count;
	int                       cpu_frequency;
	ResourceSet               slave_resources;
	std::string               task_path;

	std::string               slave_addr;
	uint16_t                  slave_port;
	std::string               master_addr;
	bool                      registered;

	int                       hb_interval;
	int                       hb_elapsed;
	int                       hb_counter;
	int                       hb_error;
	HeartbeatTimerPtr         hb_timer;

	IOContextPtr              ioctx;
	ListenerPtr               listener;

	std::mutex                exec_mutex;
	TaskExecInfoMap           exec_tasks;
};

}

#endif
