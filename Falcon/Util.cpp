#define WIN32_LEAN_AND_MEAN 
#include <Windows.h>
#include <sstream>
#include <json/json.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
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