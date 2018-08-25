#ifndef FALCON_SCHEDULER_H
#define FALCON_SCHEDULER_H

#include "Falcon.h"

namespace falcon {

class MasterServer;

class Scheduler
{
public:
	typedef std::list<std::pair<std::string, TaskList>> Table;

public:
	Scheduler(MasterServer* server);

	Table ScheduleTasks();

private:
	MasterServer* master_server;
};

}

#endif
