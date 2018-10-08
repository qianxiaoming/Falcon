#include <sstream>
#define WIN32_LEAN_AND_MEAN 
#include <Windows.h>
#include <atlbase.h>
#include <strsafe.h>
#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <curl/curl.h>
#include <json/json.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/filesystem.hpp>
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

std::string Util::JsonToString(const Json::Value& value)
{
	if (value.isNull()) return "";
	std::ostringstream oss;
	if (value.isObject()) {
		oss << "{ ";
		std::vector<std::string> names = value.getMemberNames();
		for (size_t i = 0; i < names.size(); i++) {
			oss << "\"" << names[i] << "\": " << Util::JsonToString(value[names[i]]);
			if (i < (names.size() - 1))
				oss << ",";
		}
		oss << " }";
	}
	else if (value.isArray()) {
		oss << "[ ";
		size_t count = value.size();
		for (size_t i = 0; i < count; i++) {
			oss << Util::JsonToString(value[int(i)]);
			if (i < (count - 1))
				oss << ",";
		}
		oss << " ]";
	}
	else if (value.isString())
		oss << "\"" << value.asString() << "\"";
	else
		oss << value;
	return oss.str();
}

ResourceSet Util::ParseResourcesJson(const Json::Value& value)
{
	ResourceSet resources;
	std::vector<std::string> names = value.getMemberNames();
	for (const std::string& n : names) {
		if (value[n].isInt())
			resources.Set(n.c_str(), value[n].asInt());
		else
			resources.Set(n.c_str(), value[n].asFloat());
	}
	return resources;
}

LabelList Util::ParseLabelList(const std::string& str)
{
	if (str.empty())
		return LabelList();

	LabelList labels;
	std::vector<std::string> label_strs;
	boost::split(label_strs, str, boost::is_any_of(";"));
	for (const std::string& v : label_strs) {
		if (v.empty())
			continue;
		std::vector<std::string> label;
		boost::split(label, v, boost::is_any_of("="));
		labels.insert(std::make_pair(label[0], label[1]));
	}
	return labels;
}

std::string Util::UUID()
{
	std::srand((unsigned int)time(NULL));
	// generate 6 bytes unique string
	std::ostringstream oss;
	for (int i = 0; i < 6; i++) {
		bool use_char = (std::rand() % 300) < 200;
		if (use_char)
			oss << char(std::rand() % 26 + 'a');
		else
			oss << char(std::rand() % 10 + '0');
	}
	return oss.str();
}

std::string Util::GetLastErrorMessage(int* code)
{
	DWORD dw = GetLastError();
	LPVOID lpMsgBuf = NULL;
	FormatMessage(
		FORMAT_MESSAGE_ALLOCATE_BUFFER |
		FORMAT_MESSAGE_FROM_SYSTEM |
		FORMAT_MESSAGE_IGNORE_INSERTS,
		NULL,
		dw,
		MAKELANGID(LANG_NEUTRAL, SUBLANG_DEFAULT),
		(LPTSTR)&lpMsgBuf,
		0, NULL);
	std::string errmsg = (LPTSTR)lpMsgBuf;
	LocalFree(lpMsgBuf);

	if (code)
		*code = int(dw);
	return errmsg;
}

int SqliteDB::Execute(const std::string& sql, std::string& err)
{
	std::lock_guard<std::mutex> lock(*mutex);
	char* errmsg = NULL;
	int r = sqlite3_exec(handle, sql.c_str(), NULL, NULL, &errmsg);
	if (errmsg) {
		err = errmsg;
		sqlite3_free(errmsg);
	}
	return r;
}

void Util::GetCPUInfo(std::string& processor_name, int& num_cpus, int& clock_speed)
{
	CRegKey regkey;
	LSTATUS lResult = regkey.Open(HKEY_LOCAL_MACHINE, "HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0", KEY_ALL_ACCESS); //´ò¿ª×¢²á±í¼ü  
	if (lResult != ERROR_SUCCESS) {
		LOG(ERROR) << "Unable to open registry key HARDWARE\\DESCRIPTION\\System\\CentralProcessor\\0";
		return;
	}

	char cpu_name[50] = { 0 };
	ULONG cpu_name_size = 50;
	if (ERROR_SUCCESS == regkey.QueryStringValue("ProcessorNameString", cpu_name, &cpu_name_size))
		processor_name = cpu_name;

	DWORD value;
	if (ERROR_SUCCESS == regkey.QueryDWORDValue("~MHz", value))
		clock_speed = (int)value;
	regkey.Close();

	SYSTEM_INFO si;
	memset(&si, 0, sizeof(SYSTEM_INFO));
	GetSystemInfo(&si);
	num_cpus = si.dwNumberOfProcessors;
}

int Util::GetTotalMemory()
{
	MEMORYSTATUSEX mem_status;
	mem_status.dwLength = sizeof(mem_status);
	GlobalMemoryStatusEx(&mem_status);

	return (int)(mem_status.ullTotalPhys / (1024 * 1024));
}

void Util::GetGPUInfo(std::string& gpu_name, int& num_gpus, int& gpu_cores)
{
	num_gpus = 0;
	gpu_cores = 0;
	gpu_name = "NVIDIA";
	char* gpu_env = getenv("GPU_DEVICE_INFO"); // "1:4096"
	if (gpu_env) {
		std::string str = gpu_env;
		std::string::size_type pos = str.find(':');
		if (pos == std::string::npos)
			num_gpus = std::atoi(str.c_str());
		else {
			num_gpus = std::atoi(str.substr(0, pos).c_str());
			gpu_cores = std::atoi(str.substr(pos + 1).c_str());
		}
	}
}

}
