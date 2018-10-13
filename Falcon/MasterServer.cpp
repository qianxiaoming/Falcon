#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <boost/filesystem.hpp>
#include <boost/bind.hpp>
#include "Falcon.h"
#include "HttpBase.h"
#include "MasterServer.h"
#include "Scheduler.h"
#include "Util.h"

namespace falcon {

MasterConfig::MasterConfig()
	: cluster_name("Falcon Cluster"),
	  slave_addr("0.0.0.0"),  slave_port(MASTER_SLAVE_PORT),   slave_num_threads(3), slave_heartbeat(10),
	  client_addr("0.0.0.0"), client_port(MASTER_CLIENT_PORT), client_num_threads(2),
	  dispatch_num_threads(1), dispatch_try_times(2)
{
}

MasterServer::MasterServer()
{
}

bool MasterServer::StartServer()
{
	MasterServer* server = MasterServer::Instance();
	if (!server->LoadConfiguration())
		return false;
	if (!server->RestoreHistorical())
		return false;
	if (!server->SetupSlaveHTTP())
		return false;
	if (!server->SetupClientHTTP())
		return false;
	server->SetupAPIHandler();
	return true;
}

void MasterServer::RunServer()
{
	MasterServer* server = MasterServer::Instance();
	server->Run();

	MasterServer::Destory();
}

int MasterServer::StopServer()
{
	MasterServer* server = MasterServer::Instance();
	return server->StopService();
}

const char* MasterServer::GetName()
{
	return FALCON_MASTER_SERVER_NAME;
}

static MasterServer* master_instance = nullptr;

MasterServer* MasterServer::Instance()
{
	if (master_instance == nullptr)
		master_instance = new MasterServer;
	return master_instance;
}

void MasterServer::Destory()
{
	delete master_instance;
	master_instance = nullptr;
}

static bool InitializeMasterDB(std::string db_file)
{
	sqlite3* db;
	int rc = sqlite3_open(db_file.c_str(), &db);
	if (rc) {
		LOG(ERROR) << "Can't open master database file: " << sqlite3_errmsg(db);
		sqlite3_close(db);
		return false;
	}
	std::vector<std::string> sqls = { 
		"create table Job(id text primary key, name text, type text, submit_time int, exec_time int, finish_time int, state text)",
		"create table Task(job_id text, task_id text, task_name text, state text, exit_code int, errmsg text, exec_time int, finish_time int, machine text, primary key(job_id, task_id))"};
	for (std::string& sql : sqls) {
		char* errmsg = NULL;
		int rc = sqlite3_exec(db, sql.c_str(), NULL, NULL, &errmsg);
		if (rc) {
			LOG(ERROR) << "Failed to execute sql '" << sql << "<<': " << errmsg;
			sqlite3_close(db);
			boost::filesystem::remove(db_file);
			return false;
		}
	}
	
	sqlite3_close(db);
	return true;
}

bool MasterServer::LoadConfiguration()
{
	LOG(INFO) << "Loading master server configuration...";
	const char* cluster_name = getenv("FALCON_CLUSTER");
	if (cluster_name)
		config.cluster_name = cluster_name;

	std::string db_file = Util::GetModulePath() + "/falcon_master.db";
	if (!boost::filesystem::exists(db_file)) {
		if (!InitializeMasterDB(db_file))
			return false;
	}

	int rc = sqlite3_open(db_file.c_str(), &data_state.master_db);
	if (data_state.master_db == nullptr) {
		LOG(ERROR) << "Can not open master database file: " << db_file;
		return false;
	}
	LOG(INFO) << "Configuration loaded";
	return true;
}

bool MasterServer::RestoreHistorical()
{
	LOG(INFO) << "Restoring historical jobs...";
	int total_running_jobs = 0;
	LOG(INFO) << total_running_jobs <<" running job(s) restored";
	return true;
}

bool MasterServer::SetupSlaveHTTP()
{
	LOG(INFO) << "Setup HTTP service for slaves on " << config.slave_addr << ":" << config.slave_port << "...";
	auto const address = boost::asio::ip::make_address(config.slave_addr);

	slave_ioctx = boost::make_shared<boost::asio::io_context>(config.slave_num_threads);
	boost::asio::io_context& ioc = *slave_ioctx;

	slave_listener = std::make_shared<Listener>(
		ioc,
		tcp::endpoint{ address, config.slave_port },
		boost::bind(&MasterServer::HandleSlaveRequest, this, _1, _2, _3, _4, _5));
	if (!slave_listener->IsListening()) {
		slave_listener.reset();
		return false;
	}
	slave_listener->Accept();
	LOG(INFO) << "HTTP service for slaves OK";
	return true;
}

bool MasterServer::SetupClientHTTP()
{
	LOG(INFO) << "Setup HTTP service for clients on " << config.client_addr << ":" << config.client_port << "...";
	auto const address = boost::asio::ip::make_address(config.client_addr);

	client_ioctx = boost::make_shared<boost::asio::io_context>(config.client_num_threads);
	boost::asio::io_context& ioc = *client_ioctx;

	client_listener = std::make_shared<Listener>(ioc,
		tcp::endpoint{ address, config.client_port },
		boost::bind(&MasterServer::HandleClientRequest, this, _1, _2, _3, _4, _5));
	if (!client_listener->IsListening()) {
		client_listener.reset();
		return false;
	}
	client_listener->Accept();
	LOG(INFO) << "HTTP service for clients OK";
	return true;
}

static void DispatchTaskLoop(MasterServer* server, DispatchTaskQueue& task_queue)
{
	int undispatched = 0;
	while (true) {
		DispatchTask* t = nullptr;
		task_queue.Dequeue(t);
		if (t == nullptr && server->IsStopped())
			break;
		if (t == nullptr) {
			// notify the schedule thread that all tasks have been dispatched
			task_queue.Notify(undispatched);
			undispatched = 0;
			continue;
		}
		std::unique_ptr<DispatchTask> task(t);

		LOG(INFO) << "Dispatching " << task->job_id << "." << task->task_id << " to " << task->slave_id;
		bool dispatched = false;
		Json::Value request(Json::objectValue);
		request["job_id"]  = task->job_id;
		request["task_id"] = task->task_id;
		request["content"] = task->content;
		std::string result = HttpUtil::Post(boost::str(boost::format("%s/tasks") % task->slave_id), request.toStyledString());
		Json::Value response;
		if (!Util::ParseJsonFromString(result, response))
			LOG(ERROR) << "Invalid response from slave: " << result;
		else {
			if (response.isMember("error")) {
				LOG(ERROR) << "Failed to dispatch task to slave "<<task->slave_id<<": " << response["error"].asString();
			} else {
				dispatched = true;
				TaskState state = ToTaskState(response["state"].asCString());
				TaskStatus status(state);
				status.slave_id = task->slave_id;
				if (state == TaskState::Executing) {
					status.exec_time   = response["time"].asInt64();
					status.machine     = response["machine"].asString();
				} else if (state == TaskState::Aborted) {
					status.exit_code   = response["exit_code"].asUInt();
					status.error_msg   = response["message"].asString();
					status.finish_time = response["time"].asInt64();
					status.machine     = response["machine"].asString();
				}
				server->State().UpdateTaskStatus(task->job_id, task->task_id, status);
				if (state == TaskState::Executing)
					server->State().AddExecutingTask(task->slave_id, task->job_id, task->task_id);
			}
		}
		
		if (!dispatched) {
			task->dispatch_count++;
			if (task->dispatch_count < server->GetConfig().dispatch_try_times)
				task_queue.Enqueue(task.release());
			else {
				undispatched++;
				server->State().UpdateTaskStatus(task->job_id, task->task_id, TaskStatus(TaskState::Queued));
			}
		}
	}
}

static void TaskScheduleLoop(MasterServer* server, ScheduleEventQueue& sched_queue, DispatchTaskQueue& dispatch_queue)
{
	int sched_count = 0;
	while (true) {
		sched_count++;
		LOG(INFO) << "Start schedule cycle(" << sched_count << ")...";

		Json::StreamWriterBuilder builder;
		builder["commentStyle"] = "None";
		builder["indentation"] = "";
		std::unique_ptr<Json::StreamWriter> writer(builder.newStreamWriter());

		Scheduler::Table table = Scheduler(server).ScheduleTasks();
		if (table.empty())
			LOG(INFO) << "Schedule cycle done and no task is scheduled";
		else {
			int total = 0;
			Scheduler::Table::iterator it = table.begin(), end = table.end();
			while (it != end) {
				auto it_task = std::find_if(it->second.begin(), it->second.end(),
					[](const TaskPtr& t) { return t->task_status.state == TaskState::Queued; });
				if (it_task == it->second.end())
					it++;
				else {
					std::string &job_id = (*it_task)->job_id, &task_id = (*it_task)->task_id;
					if (server->State().UpdateTaskStatus(job_id, task_id, TaskStatus(TaskState::Dispatching))) {
						DispatchTask* task = new DispatchTask(it->first, job_id, task_id);
						task->content = (*it_task)->ToJson();
						dispatch_queue.Enqueue(task);
						total++;
						LOG(INFO) << "Schedule task " << job_id << "." << task_id << " to slave " << it->first;
					}
					// move to next schedule table entry
					if (++it == end)
						it = table.begin();
				}
			}
			// put a stop sign into the queue for dispatching thread
			dispatch_queue.Enqueue(nullptr);
			// wait all task to be dispatched
			int undispatched = dispatch_queue.Wait();
			LOG(INFO) << "Schedule cycle done and " << (total - undispatched) << "/" << total << " tasks have been dispatched";
		}

		ScheduleEvent evt;
		sched_queue.wait_dequeue(evt);
		if (evt != ScheduleEvent::Stop) {
			// dequeue all schedule events in notify queue
			while (sched_queue.try_dequeue(evt)) {
				if (evt == ScheduleEvent::Stop)
					break;
			}
		}
		if (evt == ScheduleEvent::Stop) {
			LOG(INFO) << "Schedule stop event received. Stop task schedule loop now";
			break;
		}
	}
}

void MasterServer::Run()
{
	LOG(INFO) << "Master server is running...";
	int worker_threads = 4;
	std::mutex mutex;
	std::condition_variable cond;

	auto const notify_exit = [&worker_threads, &cond, &mutex]()
	{
		std::unique_lock<std::mutex> lock(mutex);
		worker_threads--;
		cond.notify_all();
	};

	auto const worker_thread_func = [&notify_exit](boost::asio::io_context* ioctx, int num_threads)
	{
		for (int i = num_threads - 1; i > 0; --i) {
			std::thread t([&ioctx] { ioctx->run(); });
			t.detach();
		}
		ioctx->run();
		notify_exit();
	};

	// run threads for clients
	std::thread client_thread(worker_thread_func, client_ioctx.get(), config.client_num_threads);
	client_thread.detach();
	
	// run threads for slaves
	std::thread slave_thread(worker_thread_func, slave_ioctx.get(), config.slave_num_threads);
	slave_thread.detach();

	// run schedule thread
	ScheduleEventQueue& sched_queue   = sched_event_queue;
	DispatchTaskQueue& dispatch_queue = dispatch_task_queue;
	auto const sched_thread_func = [&notify_exit, &sched_queue, &dispatch_queue](MasterServer* server, int delay)
	{
		std::this_thread::sleep_for(std::chrono::seconds(delay)); // sleep for a while waiting slaves to register
		TaskScheduleLoop(server, sched_queue, dispatch_queue);
		notify_exit();
	};
	std::thread sched_thread(sched_thread_func, this, config.slave_heartbeat*3);
	sched_thread.detach();

	// run dispatching thread
	auto const dispatch_thread_func = [&notify_exit, &dispatch_queue](MasterServer* server, int num_threads)
	{
		for (int i = num_threads - 1; i > 0; --i) {
			std::thread t([&server, &dispatch_queue] { DispatchTaskLoop(server, dispatch_queue); });
			t.detach();
		}
		DispatchTaskLoop(server, dispatch_queue);
		notify_exit();
	};
	std::thread dispatch_thread(dispatch_thread_func, this, config.dispatch_num_threads);
	dispatch_thread.detach();

	// wait all threads
	std::unique_lock <std::mutex> lock(mutex);
	cond.wait(lock, [&worker_threads] { return worker_threads == 0; });
	LOG(INFO) << "Master server is going to shutdown";
}

int MasterServer::StopService()
{
	is_stopped.store(true);
	// terminate scheduler thread and dispatching thread
	sched_event_queue.enqueue(ScheduleEvent::Stop);
	for (int i = 0; i < config.dispatch_num_threads; i++)
		dispatch_task_queue.Enqueue(nullptr);

	if (client_listener)
		client_listener->Stop();
	if (slave_listener)
		slave_listener->Stop();
	if (data_state.master_db) {
		sqlite3_close(data_state.master_db);
		data_state.master_db = nullptr;
	}
	return EXIT_SUCCESS;
}

void MasterServer::NotifyScheduleEvent(ScheduleEvent evt)
{
	sched_event_queue.enqueue(evt);
}

}
