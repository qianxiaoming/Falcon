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
	  slave_addr("0.0.0.0"),  slave_port(MASTER_SLAVE_PORT),   slave_num_threads(3), slave_heartbeat(5),
	  client_addr("0.0.0.0"), client_port(MASTER_CLIENT_PORT), client_num_threads(2),
	  dispatch_num_threads(2)
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
		"create table Task(job_id text, task_id text, task_name text, state text, exit_abort int, exit_code int, exec_time int, finish_time int, primary key(job_id, task_id))"};
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

static void DispatchTaskLoop(DispatchTaskQueue& task_queue, boost::function<bool(const std::string&, const std::string&, std::string&)> requeue_task)
{
	DispatchTask* task;
	while (true) {
		task = nullptr;
		task_queue.wait_dequeue(task);
		if (task == nullptr)
			break;

		LOG(INFO) << "Dispatching " << task->job_id << "." << task->task_id << " to " << task->target;
		std::this_thread::sleep_for(std::chrono::milliseconds(500));
		delete task;
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
		Scheduler::Table::iterator it = table.begin(), end = table.end();
		while (it != end) {
			auto it_task = std::find_if(it->second.begin(), it->second.end(),
				[](const TaskPtr& t) { return t->task_status.state == Task::State::Queued; });
			if (it_task == it->second.end())
				it++;
			else {
				std::string err, &job_id = (*it_task)->job_id, &task_id = (*it_task)->task_id;
				if (!server->State().SetTaskState(job_id, task_id, Task::State::Dispatching, err)) {
					LOG(ERROR) << "Unable to update task " << job_id << "." << task_id << "state to Dispatching: " << err;
				} else {
					DispatchTask* dt = new DispatchTask(it->first, job_id, task_id);
					std::ostringstream oss;
					writer->write((*it_task)->ToJson(), &oss);
					dt->content = oss.str();
					dispatch_queue.enqueue(dt);
					LOG(INFO) << "  Schedule task " << job_id << "." << task_id << " to slave " << it->first;
				}
				// move to next schedule table entry
				if (++it == end)
					it = table.begin();
			}
		}
		LOG(INFO) << "Schedule cycle done";

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
	int count = 4;
	std::mutex mutex;
	std::condition_variable cond;

	auto const notify_thread_exit = [&count, &cond, &mutex]()
	{
		std::unique_lock<std::mutex> lock(mutex);
		count--;
		cond.notify_all();
	};

	auto const worker_thread_func = [&notify_thread_exit](boost::asio::io_context* ioctx, int num_threads)
	{
		for (int i = num_threads - 1; i > 0; --i) {
			std::thread t([&ioctx] { ioctx->run(); });
			t.detach();
		}
		ioctx->run();
		notify_thread_exit();
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
	auto const sched_thread_func = [&notify_thread_exit, &sched_queue, &dispatch_queue](MasterServer* server, int delay)
	{
		std::this_thread::sleep_for(std::chrono::seconds(delay)); // sleep for a while waiting slaves to register
		TaskScheduleLoop(server, sched_queue, dispatch_queue);
		notify_thread_exit();
	};
	std::thread sched_thread(sched_thread_func, this, config.slave_heartbeat*3);
	sched_thread.detach();

	// run dispatching thread
	boost::function<bool(const std::string&, const std::string&, std::string&)> requeue_task =
		boost::bind(&DataState::SetTaskState, &data_state, _1, _2, Task::State::Queued, _3);
	auto const dispatch_thread_func = [&notify_thread_exit, &dispatch_queue, &requeue_task](int num_threads)
	{
		for (int i = num_threads - 1; i > 0; --i) {
			std::thread t([&dispatch_queue, &requeue_task] { DispatchTaskLoop(dispatch_queue, requeue_task); });
			t.detach();
		}
		DispatchTaskLoop(dispatch_queue, requeue_task);
		notify_thread_exit();
	};
	std::thread dispatch_thread(dispatch_thread_func, config.dispatch_num_threads);
	dispatch_thread.detach();

	// wait all threads
	std::unique_lock <std::mutex> lock(mutex);
	cond.wait(lock, [&count] { return count == 0; });
	LOG(INFO) << "Master server is going to shutdown";
}

int MasterServer::StopService()
{
	is_stopped.store(true);
	// terminate scheduler thread and dispatching thread
	sched_event_queue.enqueue(ScheduleEvent::Stop);
	for (int i = 0; i < config.dispatch_num_threads; i++)
		dispatch_task_queue.enqueue(nullptr);

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

JobPtr MasterServer::DataState::GetJob(const std::string& job_id) const
{
	JobList::const_iterator it = std::find_if(job_queue.begin(), job_queue.end(),
		[&job_id](const JobPtr& t) { return t->job_id == job_id; });
	if (it == job_queue.end())
		return JobPtr();
	return *it;
}

bool MasterServer::DataState::InsertNewJob(const std::string& job_id, const std::string& name, Job::Type type, const Json::Value& value, std::string& err)
{
	// save attributes of new job into database
	time_t submit_time = time(NULL);
	SqliteDB db(master_db, &db_mutex);
	std::ostringstream oss;
	oss << "insert into Job(id,name,type,submit_time,state) values('" << job_id << "','" << name << "','"
		<< ToString(type) << "'," << submit_time << ",'" << ToString(Job::State::Queued) << "')";
	if (db.Execute(oss.str(), err) != SQLITE_OK) {
		err = "Failed to write new job into database: " + err;
		return false;
	}

	// create job object and add it to queue
	JobPtr job;
	if (type == Job::Type::Batch)
		job.reset(new BatchJob(job_id, name));
	else
		job.reset(new DAGJob(job_id, name));
	job->submit_time = submit_time;
	job->Assign(value);

	// save tasks information
	TaskList::iterator it;
	while (job->NextTask(it)) {
		TaskPtr task = *it;
		std::ostringstream oss;
		oss << "insert into Task(job_id, task_id, task_name, state, exit_abort, exit_code, exec_time, finish_time) values('"
			<< job->job_id << "','" << task->task_id << "','" << task->task_name << "','" << ToString(task->task_status.state) << "',0,0,0,0)";
		if (db.Execute(oss.str(), err) != SQLITE_OK) {
			err = "Failed to write tasks into database: " + err;
			return false;
		}
	}
	db.Unlock();
	
	std::lock_guard<std::mutex> lock(queue_mutex);
	job_queue.push_back(job);
	return true;
}

void MasterServer::DataState::RegisterMachine(const std::string& name, const std::string& addr, const std::string& os, int cpu_count, int cpu_freq, const ResourceSet& resources)
{
	std::lock_guard<std::mutex> lock(machine_mutex);
	if (machines.find(name) != machines.end())
		LOG(WARNING) << "Machine named '" << name << "' already exists and will be replaced.";
	Machine mac;
	mac.name = name;
	mac.ip = addr;
	mac.os = os;
	mac.cpu_count = cpu_count;
	mac.cpu_frequency = cpu_freq;
	mac.resources = resources;
	mac.availables = resources;
	mac.state = Machine::State::Online;
	mac.online = time(NULL);
	mac.heartbeat = mac.online;
	machines[name] = mac;
}

bool MasterServer::DataState::SetTaskState(const std::string& job_id, const std::string& task_id, Task::State state, std::string& err)
{
	SqliteDB db(master_db, &db_mutex);
	std::ostringstream oss;
	oss << "update Task set state=\"" << ToString(state) << "\" where job_id=\"" << job_id << "\" and task_id=\"" << task_id << "\"";
	if (db.Execute(oss.str(), err) != SQLITE_OK)
		return false;
	db.Unlock();

	std::lock_guard<std::mutex> lock(queue_mutex);
	JobPtr job = GetJob(job_id);
	if (job) {
		if (TaskPtr task = job->GetTask(task_id))
			task->task_status.state = state;
	}
	return true;
}

}
