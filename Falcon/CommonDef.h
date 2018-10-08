#ifndef FALCON_COMMON_DEF_H
#define FALCON_COMMON_DEF_H

#include <cstdint>

namespace falcon {

#define FALCON_MASTER_SERVER_NAME "Falcon-Master"
#define FALCON_SLAVE_SERVER_NAME  "Falcon-Slave"

#define RESOURCE_CPU              "cpu"
#define RESOURCE_FREQ             "freq"
#define RESOURCE_GPU              "gpu"
#define RESOURCE_MEM              "mem"
#define RESOURCE_DISK             "disk"

const uint16_t MASTER_SLAVE_PORT  = 36780;
const uint16_t MASTER_CLIENT_PORT = 36781;
const uint16_t SLAVE_LISTEN_PORT  = 36782;

enum class TaskState    { Queued, Dispatching, Executing, Completed, Failed, Aborted, Terminated };
enum class JobType      { Batch, DAG };
enum class JobState     { Queued, Waiting, Executing, Halted, Completed, Failed, Terminated };
enum class MachineState { Online, Offline, Unknown };

}

#endif
