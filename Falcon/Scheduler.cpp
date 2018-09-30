#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include "Scheduler.h"
#include "MasterServer.h"

namespace falcon {

Scheduler::Scheduler(MasterServer* server) : master_server(server), log_sched(false)
{
	if (getenv("FALCON_LOG_SCHED") != NULL)
		log_sched = strcmp(getenv("FALCON_LOG_SCHED"), "YES") == 0 || strcmp(getenv("FALCON_LOG_SCHED"), "TRUE") == 0;
}

Scheduler::Table Scheduler::ScheduleTasks()
{
	std::list<JobPtr> jobs;
	std::list<Machine>  machines;

	// make a copy for all schedulable jobs and online machines
	MasterServer::DataState& state = master_server->State();
	do {
		std::lock_guard<std::mutex> lock_job(state.queue_mutex);
		std::copy_if(state.job_queue.begin(), state.job_queue.end(), std::back_inserter(jobs),
			[](const JobPtr& job) { return job->job_state == Job::State::Queued || 
			                               job->job_state == Job::State::Executing; });
		
		std::lock_guard<std::mutex> lock_mac(state.machine_mutex);
		for (const auto& mac : state.machines) {
			if (mac.second->state == Machine::State::Online)
				machines.push_back(*mac.second);
		}
	} while (false);

	// sort jobs according to their priority and submit time
	jobs.sort([](JobPtr l, JobPtr r) {
		if (l->job_priority > r->job_priority)
			return true;
		if (l->job_priority == r->job_priority)
			return l->submit_time < r->submit_time;
		return false;
	});

	// schedule tasks as many as possible
	Table result;
	while (true) {
		// find next job should be scheduled
		JobPtr sched_job;
		TaskList sched_tasks;
		for (JobList::iterator it = jobs.begin(); it != jobs.end(); it++) {
			JobPtr job = *it;
			if (!job)
				continue;
			job->GetTaskList(sched_tasks, [](const TaskPtr& t) { return t->task_status.state == Task::State::Queued; });
			if (!sched_tasks.empty()) {
				sched_job = job;
				jobs.erase(it);
				break;
			}
		}
		if (!sched_job)
			break; // no more job to schedule

		for (const TaskPtr& task : sched_tasks) {
			std::vector<Machine*> available_macs;
			// filter slaves according to task constraints
			for (Machine& mac : machines) {
				// filter by task labels
				bool not_match = false;
				for (const auto& v : task->task_labels) {
					LabelList::const_iterator label_iter = mac.labels.find(v.first);
					if (label_iter == mac.labels.end() || label_iter->second != v.second) {
						not_match = true;
						break;
					}
				}
				if (not_match)
					continue;

				// filter slaves according to task resource requests
				if (!mac.availables.IsSatisfiable(task->resources))
					continue;
				available_macs.push_back(&mac);
			}

			// select best slave for this task
			if (available_macs.empty()) {
				if (log_sched)
					LOG(INFO) << "No suitable machine found to execute task "<<sched_job->job_id<<"."<<task->task_id;
			} else {
				struct PriorityInfo
				{
					Machine* mac;
					ResourceSet::Proportion props;
				};
				std::vector<PriorityInfo> mac_caps(available_macs.size());
				// calculate some metrics for later sort
				for (size_t i = 0; i < mac_caps.size(); i++) {
					mac_caps[i].mac = available_macs[i];
					mac_caps[i].props = available_macs[i]->availables / available_macs[i]->resources;
				}
				
				std::sort(mac_caps.begin(), mac_caps.end(), [&task](const PriorityInfo& l, const PriorityInfo& r) {
					if (task->resources.Get(RESOURCE_GPU, 0) != 0) {
						if (l.props.find(RESOURCE_GPU)->second > r.props.find(RESOURCE_GPU)->second) return true;
						if (l.props.find(RESOURCE_GPU)->second < r.props.find(RESOURCE_GPU)->second) return false;
					}
					if (l.props.find(RESOURCE_CPU)->second > r.props.find(RESOURCE_CPU)->second) return true;
					if (l.props.find(RESOURCE_CPU)->second < r.props.find(RESOURCE_CPU)->second) return false;
					if (l.props.find(RESOURCE_FREQ)->second > r.props.find(RESOURCE_FREQ)->second) return true;
					if (l.props.find(RESOURCE_FREQ)->second < r.props.find(RESOURCE_FREQ)->second) return false;
					return l.mac->cpu_frequency > r.mac->cpu_frequency;
				});
				std::string target = mac_caps[0].mac->id;

				// put this task into result table
				Table::iterator res_iter = result.find(target);
				if (res_iter != result.end())
					res_iter->second.push_back(task);
				else {
					result[target] = TaskList();
					result[target].push_back(task);
				}

				// reduce the resouce amount for selected slave
				mac_caps[0].mac->availables -= task->resources;
			}
		}
	}
	return result;
}

}
