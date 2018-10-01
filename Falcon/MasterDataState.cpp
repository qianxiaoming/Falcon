#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include "Falcon.h"
#include "MasterServer.h"

namespace falcon {

JobPtr MasterServer::DataState::GetJob(const std::string& job_id) const
{
	JobList::const_iterator it = std::find_if(job_queue.begin(), job_queue.end(),
		[&job_id](const JobPtr& t) { return t->job_id == job_id; });
	if (it == job_queue.end())
		return JobPtr();
	return *it;
}

MachinePtr MasterServer::DataState::GetMachine(const std::string& slave_id) const
{
	MachineMap::const_iterator it = machines.find(slave_id);
	if (it == machines.end())
		return nullptr;
	return it->second;
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
	TaskList tasks;
	job->GetTaskList(tasks);
	for (const TaskPtr& task : tasks) {
		std::ostringstream oss;
		oss << "insert into Task(job_id, task_id, task_name, state, exit_code, errmsg, exec_time, finish_time) values('"
			<< job->job_id << "','" << task->task_id << "','" << task->task_name << "','" << ToString(task->task_status.state) << "',0,\"\",0,0)";
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

std::string MasterServer::DataState::RegisterMachine(const std::string& name, const std::string& addr, uint16_t port, const std::string& os, int cpu_count, int cpu_freq, const ResourceSet& resources)
{
	std::lock_guard<std::mutex> lock(machine_mutex);
	MachinePtr mac(new Machine);
	mac->id = boost::str(boost::format("%s:%d") % addr % int(port));
	if (machines.find(mac->id) != machines.end())
		LOG(WARNING) << "Machine identified by '" << mac->id << "' already exists and will be replaced.";
	mac->name = name;
	mac->address = addr;
	mac->port = port;
	mac->os = os;
	mac->cpu_count = cpu_count;
	mac->cpu_frequency = cpu_freq;
	mac->resources = resources;
	mac->availables = resources;
	mac->state = Machine::State::Online;
	mac->online = time(NULL);
	mac->heartbeat = mac->online;
	machines[mac->id] = mac;
	return mac->id;
}

bool MasterServer::DataState::UpdateTaskStatus(const std::string& job_id, const std::string& task_id, const Task::Status& status)
{
	JobPtr job;
	Job::State old_state = Job::State::Queued, new_state = Job::State::Queued;
	if (!job) {
		std::lock_guard<std::mutex> lock(queue_mutex);
		job = GetJob(job_id);
		if (!job) {
			LOG(ERROR) << "No job found for id " << job_id;
			return false;
		}

		if (TaskPtr task = job->GetTask(task_id)) {
			task->task_status.state = status.state;
			if (status.state == Task::State::Executing) {
				if (status.exec_time > 0)
					task->task_status.exec_time = status.exec_time;
				if (!status.machine.empty())
					task->task_status.machine = status.machine;
				if (!status.slave_id.empty())
					task->task_status.slave_id = status.slave_id;
				task->task_status.progress = status.progress;
				if (!status.exec_tip.empty())
					task->task_status.exec_tip = status.exec_tip;
			}
			else if (status.state == Task::State::Completed || status.state == Task::State::Failed) {
				task->task_status.finish_time = status.finish_time;
				task->task_status.exit_code = status.exit_code;
			}
			else if (status.state == Task::State::Aborted) {
				if (!status.machine.empty())
					task->task_status.machine = status.machine;
				if (!status.slave_id.empty())
					task->task_status.slave_id = status.slave_id;
				task->task_status.finish_time = status.finish_time;
				task->task_status.exit_code = status.exit_code;
				task->task_status.error_msg = status.error_msg;
			}
			else if (status.state == Task::State::Terminated)
				task->task_status.finish_time = status.finish_time;

			if (status.IsFinished()) {
				std::lock_guard<std::mutex> lock_macs(machine_mutex);
				MachinePtr mac = GetMachine(task->task_status.slave_id);
				if (mac)
					mac->availables += task->resources;
			}
		}

		// update job state according to tasks
		if (status.state != Task::State::Dispatching) {
			old_state = job->job_state;
			new_state = job->UpdateCurrentState();
			if (old_state != new_state)
				LOG(INFO) << "Job " << job->job_name << "(" << job->job_id << ")'s state is changed to " << ToString(new_state);
		}
	}

	SqliteDB db(master_db, &db_mutex);
	std::ostringstream oss;
	oss << "update Task set state=\"" << ToString(status.state) << "\"";
	if (status.state == Task::State::Executing)
		oss << ",exec_time=" << status.exec_time << ",finish_time=0,exit_code=0,errmsg=\"\",machine=\"" << status.machine << "\"";
	else if (status.state == Task::State::Completed || status.state == Task::State::Failed)
		oss << ",finish_time=" << status.finish_time << ",exit_code=" << status.exit_code << ",errmsg=\"\"";
	else if (status.state == Task::State::Aborted)
		oss << ",finish_time=" << status.finish_time << ",errmsg=\"" << status.error_msg << "\",exit_code=0,machine=\"" << status.machine << "\"";
	else if (status.state == Task::State::Terminated)
		oss << ",finish_time=" << status.finish_time << ",errmsg=\"\",exit_code=0";
	else
		oss << ",exec_time=0,finish_time=0,exit_code=0,errmsg=\"\",machine=\"\"";
	oss << " where job_id=\"" << job_id << "\" and task_id=\"" << task_id << "\"";
	std::string err;
	if (db.Execute(oss.str(), err) != SQLITE_OK) {
		LOG(ERROR) << "Failed to update status for task " << job_id << "." << task_id << " to " << ToString(status.state) << ": " << err;
		return false;
	}

	if (old_state != new_state) {
		std::string sql;
		if (new_state == Job::State::Executing)
			sql = boost::str(boost::format("update Job set state=\"%s\",exec_time=%d where id=\"%s\"") % ToString(new_state) % status.exec_time % job_id);
		else if (new_state == Job::State::Completed || new_state == Job::State::Failed || new_state == Job::State::Terminated)
			sql = boost::str(boost::format("update Job set state=\"%s\",finish_time=%d where id=\"%s\"") % ToString(new_state) % status.finish_time % job_id);
		else
			sql = boost::str(boost::format("update Job set state=\"%s\" where id=\"%s\"") % ToString(new_state) % job_id);
		if (db.Execute(oss.str(), err) != SQLITE_OK)
			LOG(ERROR) << "Failed to update state for job " << job_id << " to " << ToString(new_state) << ": " << err;
	}
	return true;
}

void MasterServer::DataState::AddExecutingTask(const std::string& slave_id, const std::string& job_id, const std::string& task_id)
{
	std::lock_guard<std::mutex> lock_queue(queue_mutex), lock_macs(machine_mutex);
	JobPtr job = GetJob(job_id);
	if (!job) return;
	TaskPtr task = job->GetTask(task_id);
	if (!task) return;
	MachinePtr mac = GetMachine(slave_id);
	if (mac) {
		mac->availables -= task->resources;
		LOG(INFO) << "Machine " << slave_id << ": " << mac->availables.ToString();
	}
}

bool MasterServer::DataState::Heartbeat(const std::string& slave_id, const Json::Value& updates, int& finished)
{
	std::unique_lock<std::mutex> lock(machine_mutex);
	MachineMap::iterator it = machines.find(slave_id);
	if (it == machines.end())
		return false;
	it->second->heartbeat = time(NULL);
	lock.unlock();

	int update_count = updates.size();
	for (int i = 0; i < update_count; i++) {
		const Json::Value& t = updates[i];
		Task::Status status(FromString<Task::State>(t["state"].asCString()));
		status.progress = t["progress"].asInt();
		status.exec_tip = t["tiptext"].asString();
		if (t.isMember("exit_code"))
			status.exit_code = t["exit_code"].asUInt();
		if (t.isMember("error_msg"))
			status.error_msg = t["error_msg"].asString();
		if (status.IsFinished()) {
			finished++;
			status.finish_time = time(NULL);
			LOG(INFO) << "Task " << t["job_id"].asString() << "." << t["task_id"].asString() << " is finished with exit code "
				<< status.exit_code << ": " << status.error_msg;
		}
		UpdateTaskStatus(t["job_id"].asString(), t["task_id"].asString(), status);
	}
	return true;
}

}
