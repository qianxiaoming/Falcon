#define WIN32_LEAN_AND_MEAN 
#include <Windows.h>
#include <sstream>
#include <json/json.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "Falcon.h"
#include "Util.h"

namespace falcon {

std::string Util::GetModulePath()
{
	char module_name[256] = { 0 };
	::GetModuleFileNameA(NULL, module_name, 256);
	if (char* pos = strrchr(module_name, '\\')) {
		*pos = 0;
		return module_name;
	}
	return "";
}

bool Util::ParseJsonFromString(const std::string& json, Json::Value& value)
{
	std::istringstream iss(json);
	Json::CharReaderBuilder builder;
	builder["collectComments"] = false;
	std::string errs;
	if (!Json::parseFromStream(builder, iss, &value, &errs))
		return false;
	return true;
}

ResourceMap Util::ParseResourcesJson(const Json::Value& value)
{
	ResourceMap resmap = { { RESOURCE_CPU,  Resource(RESOURCE_CPU,  Resource::Type::Float, DEFAULT_CPU_USAGE) },
	{ RESOURCE_GPU,  Resource(RESOURCE_GPU,  Resource::Type::Int,   DEFAULT_GPU_USAGE) },
	{ RESOURCE_MEM,  Resource(RESOURCE_MEM,  Resource::Type::Int,   DEFAULT_MEM_USAGE) },
	{ RESOURCE_DISK, Resource(RESOURCE_DISK, Resource::Type::Int,   DEFAULT_DISK_USAGE) } };
	std::vector<std::string> names = value.getMemberNames();
	for (const std::string& n : names) {
		ResourceMap::iterator it = resmap.find(n);
		if (it != resmap.end()) {
			if (it->second.type == Resource::Type::Int)
				it->second.amount.ival = value[n].asInt();
			else
				it->second.amount.fval = value[n].asFloat();
		}
		else {
			if (value[n].isInt())
				resmap.insert(std::make_pair(n, Resource(n.c_str(), Resource::Type::Int, value[n].asInt())));
			else
				resmap.insert(std::make_pair(n, Resource(n.c_str(), Resource::Type::Float, value[n].asFloat())));
		}
	}
	return std::move(resmap);
}

std::string Util::UUID()
{
	std::ostringstream oss;
	oss << boost::uuids::random_generator()();
	return oss.str();
}

bool SqliteDB::Execute(const std::string& sql, std::string& err)
{
	std::lock_guard<std::mutex> lock(*mutex);
	char* errmsg = NULL;
	int r = sqlite3_exec(handle, sql.c_str(), NULL, NULL, &errmsg);
	if (errmsg) {
		err = errmsg;
		sqlite3_free(errmsg);
	}
	return r == SQLITE_OK;
}

}