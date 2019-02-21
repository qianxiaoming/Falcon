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

bool MasterServer::DataState::InsertNewJob(const std::string& job_id, const std::string& name, JobType type, const Json::Value& value, std::string& err)
{
	// save attributes of new job into database
	time_t submit_time = time(NULL);
	SqliteDB db(master_db, &db_mutex);
	std::ostringstream oss;
	oss << "insert into Job(id,name,type,submit_time,state) values('" << job_id << "','" << name << "','"
		<< ToString(type) << "'," << submit_time << ",'" << ToString(JobState::Queued) << "')";
	if (db.Execute(oss.str(), err) != SQLITE_OK) {
		err = "Failed to write new job into database: " + err;
		return false;
	}

	// create job object and add it to queue
	JobPtr job;
	if (type == JobType::Batch)
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
	mac->state = MachineState::Online;
	mac->online = time(NULL);
	mac->heartbeat = mac->online;
	machines[mac->id] = mac;
	return mac->id;
}

bool MasterServer::DataState::UpdateTaskStatus(const std::string& job_id, const std::string& task_id, const TaskStatus& status)
{
	JobPtr job;
	JobState old_state = JobState::Queued, new_state = JobState::Queued;
	if (!job) {
		std::lock_guard<std::mutex> lock(queue_mutex);
		job = GetJob(job_id);
		if (!job) {
			LOG(ERROR) << "No job found for id " << job_id;
			return false;
		}

		if (TaskPtr task = job->GetTask(task_id)) {
			task->task_status.state = status.state;
			if (status.state == TaskState::Executing) {
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
			else if (status.state == TaskState::Completed || status.state == TaskState::Failed) {
				if (status.state == TaskState::Completed)
					task->task_status.progress = status.progress;
				task->task_status.finish_time = status.finish_time;
				task->task_status.exit_code = status.exit_code;
			}
			else if (status.state == TaskState::Aborted) {
				if (!status.machine.empty())
					task->task_status.machine = status.machine;
				if (!status.slave_id.empty())
					task->task_status.slave_id = status.slave_id;
				task->task_status.finish_time = status.finish_time;
				task->task_status.exit_code = status.exit_code;
				task->task_status.error_msg = status.error_msg;
			}
			else if (status.state == TaskState::Terminated)
				task->task_status.finish_time = status.finish_time;

			if (status.IsFinished()) {
				std::lock_guard<std::mutex> lock_macs(machine_mutex);
				MachinePtr mac = GetMachine(task->task_status.slave_id);
				if (mac)
					mac->availables += task->resources;
			}
		}

		// update job state according to tasks
		if (status.state != TaskState::Dispatching) {
			old_state = job->job_state;
			new_state = job->UpdateCurrentState();
			if (old_state != new_state)
				LOG(INFO) << "Job " << job->job_name << "(" << job->job_id << ")'s state is changed to " << ToString(new_state);
		}
	}

	SqliteDB db(master_db, &db_mutex);
	std::ostringstream oss;
	oss << "update Task set state=\"" << ToString(status.state) << "\"";
	if (status.state == TaskState::Executing)
		oss << ",exec_time=" << status.exec_time << ",finish_time=0,exit_code=0,errmsg=\"\",machine=\"" << status.machine << "\"";
	else if (status.state == TaskState::Completed || status.state == TaskState::Failed)
		oss << ",finish_time=" << status.finish_time << ",exit_code=" << status.exit_code << ",errmsg=\"\"";
	else if (status.state == TaskState::Aborted)
		oss << ",finish_time=" << status.finish_time << ",errmsg=\"" << status.error_msg << "\",exit_code=0,machine=\"" << status.machine << "\"";
	else if (status.state == TaskState::Terminated)
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
		LOG(INFO) << "The state of job " << job_id << " is set to " << ToString(new_state);
		std::string sql;
		if (new_state == JobState::Executing)
			sql = boost::str(boost::format("update Job set state=\"%s\",exec_time=%d where id=\"%s\"") % ToString(new_state) % status.exec_time % job_id);
		else if (new_state == JobState::Completed || new_state == JobState::Failed || new_state == JobState::Terminated)
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
		TaskStatus status(ToTaskState(t["state"].asCString()));
		status.progress = t["progress"].asInt();
		if (status.state == TaskState::Completed)
			status.progress = 100;
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

void MasterServer::DataState::GetExecutingTasks(TaskList& tasks, const std::string& job_id)
{
	std::lock_guard<std::mutex> lock_queue(queue_mutex);
	for (JobPtr& job : job_queue) {
		if (job->job_state != JobState::Executing)
			continue;
		if (!job_id.empty() && job->job_id != job_id)
			continue;
		job->GetTaskList(tasks, [](const TaskPtr& t) { return t->task_status.state == TaskState::Executing; });
	}
}

bool MasterServer::DataState::SetJobSchedulable(const std::string& job_id, bool schedulable)
{
	std::lock_guard<std::mutex> lock_queue(queue_mutex);
	JobPtr job = GetJob(job_id);
	if (!job)
		return false;
	job->SetSchedulable(schedulable);
	return true;
}

bool MasterServer::DataState::QueryJobsJson(const std::vector<std::string>& ids, Json::Value& result)
{
	std::lock_guard<std::mutex> lock_queue(queue_mutex);
	int index = 0;
	if (ids.empty()) {
		result.resize(Json::Value::ArrayIndex(job_queue.size()));
		for (JobPtr job : job_queue) {
			result[index] = job->ToJson();
			index++;
		}
	} else {
		result.resize(Json::Value::ArrayIndex(ids.size()));
		for (const std::string& id : ids) {
			JobList::iterator it = std::find_if(job_queue.begin(), job_queue.end(), [&id](JobPtr job) { return job->job_id == id; });
			if (it != job_queue.end())
				result[index] = (*it)->ToJson();
			index++;
		}
	}
	return true;
}

bool MasterServer::DataState::QueryTasksJson(std::string job_id, const std::vector<std::string>& task_ids, Json::Value& result)
{
	TaskList tasks;
	if (tasks.empty()) {
		std::lock_guard<std::mutex> lock_queue(queue_mutex);
		JobList::iterator it = std::find_if(job_queue.begin(), job_queue.end(), [&job_id](JobPtr job) { return job->job_id == job_id; });
		if (it == job_queue.end())
			return false;

		if (task_ids.empty())
			(*it)->GetTaskList(tasks);
		else {
			for (const std::string& id : task_ids) {
				TaskPtr task = (*it)->GetTask(id);
				if (task)
					tasks.push_back(task);
			}
		}
	}
	if (tasks.empty())
		return true;
	result.resize(Json::Value::ArrayIndex(tasks.size()));
	int index = 0;
	for (TaskPtr task : tasks) {
		Json::Value value(Json::objectValue);
		value["id"] = task->task_id;
		value["name"] = task->task_name;
		value["state"] = ToString(task->task_status.state);
		value["progress"] = task->task_status.progress;
		value["exit_code"] = task->task_status.exit_code;
		if (!task->task_status.machine.empty())
			value["node"] = task->task_status.machine;
		if (!task->task_status.error_msg.empty())
			value["message"] = task->task_status.error_msg;
		if (task->task_status.exec_time != 0)
			value["start_time"] = task->task_status.exec_time;
		if (task->task_status.finish_time != 0)
			value["finish_time"] = task->task_status.finish_time;
		result[index++] = value;
	}
	return true;
}

bool MasterServer::DataState::QueryNodesJson(Json::Value& result)
{
	std::lock_guard<std::mutex> lock_queue(machine_mutex);
	result.resize(Json::Value::ArrayIndex(machines.size()));
	int index = 0;
	for (MachineMap::iterator it = machines.begin(); it != machines.end(); it++, index++) {
		MachinePtr mac = it->second;
		Json::Value val(Json::objectValue);
		val["name"]      = mac->name;
		val["address"]   = mac->address;
		val["os"]        = mac->os;
		val["state"]     = ToString(mac->state);
		val["online"]    = mac->online;
		val["labels"]    = ToString(mac->labels);
		val["resources"] = mac->resources.ToJson();
		result[index] = val;
	}
	return true;
}

}
