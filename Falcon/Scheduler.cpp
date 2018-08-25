#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include "Scheduler.h"
#include "MasterServer.h"

namespace falcon {

Scheduler::Scheduler(MasterServer* server) : master_server(server)
{

}

Scheduler::Table Scheduler::ScheduleTasks()
{
	return Table();
}

}
