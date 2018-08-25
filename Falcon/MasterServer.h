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

	std::string slave_addr;
	unsigned short slave_port;
	int slave_num_threads;

	std::string client_addr;
	unsigned short client_port;
	int client_num_threads;

	int dispatch_num_threads;
};

enum class ScheduleEvent { Stop, JobSubmit, SlaveJoin };
typedef ::moodycamel::BlockingConcurrentQueue<ScheduleEvent> ScheduleEventQueue;

struct DispatchTask
{
	DispatchTask() : dispatch_count(0) { }

	int dispatch_count;
	std::string target;
	std::string job_id;
	std::string task_id;
	std::string content;
};
typedef ::moodycamel::BlockingConcurrentQueue<DispatchTask*> DispatchTaskQueue;

typedef boost::shared_ptr<boost::asio::io_context> IOContextPtr;

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
		http::verb verb,
		const std::string& target,
		const std::string& body,
		http::status& status);

	std::string HandleSlaveRequest(
		http::verb verb,
		const std::string& target,
		const std::string& body,
		http::status& status);

	void NotifyScheduleEvent(ScheduleEvent evt);

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

		bool InsertNewJob(std::string job_id, std::string name, Job::Type type, const Json::Value& value, std::string& err);

		bool SetTaskState(std::string job_id, std::string task_id, Task::State state, std::string& err);
	};
	DataState data_state;

	DataState& GetDataState() { return data_state; }

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
