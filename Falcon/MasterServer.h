#ifndef FALCON_MASTER_SERVER_H
#define FALCON_MASTER_SERVER_H

#include <string>
#include <boost/smart_ptr.hpp>
#include <boost/asio/io_context.hpp>
#include "blockingconcurrentqueue.h"
#include "HttpBase.h"
#include "ServerBase.h"
#include "Util.h"

namespace falcon {

struct MasterConfig
{
	MasterConfig();

	std::string cluster_name;

	std::string slave_addr;
	unsigned short slave_port;
	int slave_num_threads;
	int slave_heartbeat;

	std::string client_addr;
	unsigned short client_port;
	int client_num_threads;

	unsigned short slave_listen_port;

	int dispatch_num_threads;
	int dispatch_try_times;
};

enum class ScheduleEvent { Stop, JobSubmit, SlaveJoin };
typedef ::moodycamel::BlockingConcurrentQueue<ScheduleEvent> ScheduleEventQueue;

struct DispatchTask
{
	DispatchTask(const std::string& target, const std::string& job_id, const std::string& task_id)
		: dispatch_count(0), target(target), job_id(job_id), task_id(task_id) { }
	int dispatch_count;
	std::string target;
	std::string job_id;
	std::string task_id;
	Json::Value content;
};

struct DispatchTaskQueue
{
	void Dequeue(DispatchTask*& task) { tasks.wait_dequeue(task); }
	void Enqueue(DispatchTask* task) { tasks.enqueue(task); }
	void Notify(int undispatched) { done.enqueue(undispatched); }
	int  Wait() { int v; done.wait_dequeue(v); return v; }

	::moodycamel::BlockingConcurrentQueue<DispatchTask*> tasks;
	::moodycamel::BlockingConcurrentQueue<int> done;
};

class MasterServer : public ServerBase
{
public:
	MasterServer();

	static MasterServer* Instance();

	static void Destory();

	virtual bool StartServer();

	virtual void RunServer();

	virtual int StopServer();

	virtual const char* GetName();

public:
	bool LoadConfiguration();

	bool RestoreHistorical();

	bool SetupSlaveHTTP();

	bool SetupClientHTTP();

	void Run();

	int StopService();

public:
	std::string HandleClientRequest(
		const std::string& remote_addr,
		http::verb verb,
		const std::string& target,
		const std::string& body,
		http::status& status);

	std::string HandleSlaveRequest(
		const std::string& remote_addr,
		http::verb verb,
		const std::string& target,
		const std::string& body,
		http::status& status);

	void NotifyScheduleEvent(ScheduleEvent evt);

	MasterConfig& GetConfig() { return config; }

public:
	struct DataState
	{
		DataState() : master_db(NULL) { }

		std::mutex db_mutex;
		sqlite3*   master_db;

		std::mutex queue_mutex;
		JobList    job_queue;

		std::mutex machine_mutex;
		MachineMap machines;

		std::mutex prog_mutex;
		Json::Value progress;

		JobPtr GetJob(const std::string& job_id) const;
		MachinePtr GetMachine(const std::string& ip) const;
		bool InsertNewJob(const std::string& job_id, const std::string& name, Job::Type type, const Json::Value& value, std::string& err);
		void RegisterMachine(const std::string& name, const std::string& addr, const std::string& os, int cpu_count, int cpu_freq, const ResourceSet& resources);
		bool UpdateTaskStatus(const std::string& job_id, const std::string& task_id, const Task::Status& status);
		void AddExecutingTask(const std::string& ip, const std::string& job_id, const std::string& task_id);
		void Heartbeat(const std::string& ip);
	};
	DataState data_state;

	DataState& State() { return data_state; }

private:
	void SetupAPIHandler();

	MasterConfig       config;

	IOContextPtr       client_ioctx;
	ListenerPtr        client_listener;

	IOContextPtr       slave_ioctx;
	ListenerPtr        slave_listener;

	ScheduleEventQueue sched_event_queue;
	DispatchTaskQueue  dispatch_task_queue;
};

}

#endif
