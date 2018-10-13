#ifndef FALCON_COMMON_DEF_H
#define FALCON_COMMON_DEF_H

#include <cstdint>
#include <map>
#include <string>
#include <memory>
#include <list>

#if defined(WIN32) || defined(_WINDOWS)
#if defined(LIBFALCON_EXPORTS)
#define FALCON_API __declspec(dllexport)
#else
#define FALCON_API __declspec(dllimport)
#pragma comment(lib,"libfalcon.lib")
#endif
#pragma warning(disable: 4251 4275)
#else
#define FALCON_API
#endif

namespace falcon {

#define RESOURCE_CPU              "cpu"
#define RESOURCE_FREQ             "freq"
#define RESOURCE_GPU              "gpu"
#define RESOURCE_MEM              "mem"
#define RESOURCE_DISK             "disk"

const float DEFAULT_CPU_USAGE     = 1.0;  // 1 cpu for each task
const int   DEFAULT_FREQ_USAGE    = 2400; // 2.4GHz for each task
const int   DEFAULT_GPU_USAGE     = 0;    // no gpu used for each task
const int   DEFAULT_MEM_USAGE     = 256;  // 256M memory for each task
const int   DEFAULT_DISK_USAGE    = 200;  // 200M local disk for each task

const uint16_t MASTER_SLAVE_PORT  = 36780;
const uint16_t MASTER_CLIENT_PORT = 36781;
const uint16_t SLAVE_LISTEN_PORT  = 36782;

typedef std::map<std::string, std::string> LabelList;

enum class TaskState    { Queued, Dispatching, Executing, Completed, Failed, Aborted, Terminated };
enum class JobType      { Batch, DAG };
enum class JobState     { Queued, Waiting, Executing, Halted, Completed, Failed, Terminated };
enum class MachineState { Online, Offline, Unknown };

FALCON_API std::string ToString(const LabelList& labels);
FALCON_API const char* ToString(JobType type);
FALCON_API const char* ToString(JobState state);
FALCON_API const char* ToString(TaskState state);
FALCON_API JobType ToJobType(const char* type);
FALCON_API JobState ToJobState(const char* state);
FALCON_API TaskState ToTaskState(const char* state);

/**
* @brief Resources required by tasks or owned by machines
*/
struct FALCON_API ResourceClaim
{
	enum class ValueType { Int, Float };
	struct Value
	{
		ValueType type;
		union {
			int ival;
			float fval;
		} value;

		Value() : type(ValueType::Int) { value.ival = 0; }
		Value(int v) : type(ValueType::Int) { value.ival = v; }
		Value(float v) : type(ValueType::Float) { value.fval = v; }
	};

	ResourceClaim();
	ResourceClaim(const ResourceClaim& claim);
	~ResourceClaim();

	std::string ToString() const;

	bool Exists(const std::string& name) const;
	bool Exists(const char* name) const;

	ResourceClaim& Set(const std::string& name, int val);
	ResourceClaim& Set(const std::string& name, float val);

	int Get(const std::string& name, int val) const;
	float Get(const std::string& name, float val) const;

	typedef std::map<std::string, Value>::iterator iterator;
	typedef std::map<std::string, Value>::const_iterator const_iterator;
	std::map<std::string, Value> items;
};

}

#endif
