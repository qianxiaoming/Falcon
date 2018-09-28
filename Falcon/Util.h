#ifndef FALCON_UTIL_H
#define FALCON_UTIL_H

#include <string>
#include <mutex>
#include <json/json.h>
#include "sqlite3.h"
#include "ServerBase.h"

namespace falcon {

struct Util
{
	static std::string GetModulePath();

	static bool ParseJsonFromString(const std::string& json, Json::Value& value);

	static ResourceSet ParseResourcesJson(const Json::Value& value);

	static std::string UUID();

	static LabelList ParseLabelList(const std::string& str);

	static void GetCPUInfo(std::string& processor_name, int& num_cpus, int& clock_speed);

	static int GetTotalMemory();

	static void GetGPUInfo(std::string& gpu_name, int& num_gpus, int& gpu_cores);

	static std::string Util::GetLastErrorMessage();
};

class HttpUtil
{
public:
	static std::string Get(const std::string& url, char* ret_head = NULL);

	static std::string Post(const std::string& url, const std::string& content);

	static std::string Put(const std::string& url, const std::string& content);

	static std::string Delete(const std::string& url, const std::string& content = "");

	static std::string HttpUtil::ParseHttpURL(const std::string& target, size_t offset, URLParamMap& params);
};

struct LockGuard
{
	LockGuard(std::mutex* m) : mutex(m), locked(false) { }
	~LockGuard()
	{
		Unlock();
	}
	void Lock()
	{
		if (!locked) {
			mutex->lock();
			locked = true;
		}
	}
	void Unlock()
	{
		if (locked)
			mutex->unlock();
		locked = false;
	}
	std::mutex* mutex;
	bool locked;
};

struct SqliteDB : public LockGuard
{
	SqliteDB(sqlite3* h, std::mutex* m) : LockGuard(m), handle(h) { }
	operator sqlite3*() { Lock(); return handle; }
	int Execute(const std::string& sql, std::string& err);

	sqlite3* handle;
};

}

#endif
