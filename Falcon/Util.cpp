#define WIN32_LEAN_AND_MEAN 
#include <Windows.h>
#include <atlbase.h>
#include <sstream>
#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <curl/curl.h>
#include <json/json.h>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <boost/algorithm/string.hpp>
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
	LabelList labels;
	std::vector<std::string> label_strs;
	boost::split(label_strs, str, boost::is_any_of(";"));
	for (const std::string& v : label_strs) {
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

static size_t HttpWriter(char *data, size_t size, size_t nmemb, std::string *writerData)
{
	size_t sizes = size * nmemb;
	if (writerData == NULL)
		return 0;
	writerData->append(data, sizes);
	return sizes;
}

static CURLcode TryHttpOperation(const std::string& url, CURL *curl, int count, int max_delay)
{
	LOG(ERROR) << "Failed to perform request to " << url << ", try " << count << " times...";
	CURLcode code;
	srand((unsigned)time(NULL));
	for (int i = 0; i < count; ++i) {
		int delay = rand() % max_delay;
		if (delay == 0) delay = max_delay/2;
		std::this_thread::sleep_for(std::chrono::seconds(delay));
		code = curl_easy_perform(curl);
		if (code == CURLE_OK)
			break;
	}
	return code;
}

std::string HttpUtil::Get(const std::string& url, char* ret_head)
{
	std::string result;
	CURL *curl = curl_easy_init();
	if (curl) {
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 120L);
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 60L);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HttpWriter);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

		CURLcode code = curl_easy_perform(curl);
		if (code != CURLE_OK)
			code = TryHttpOperation(url, curl, 3, 5);

		if (code != CURLE_OK) {
			Json::Value ret;
			ret["error"] = curl_easy_strerror(code);
			result = ret.toStyledString();
		}
		curl_easy_cleanup(curl);
	}
	return result;
}

std::string HttpUtil::Post(const std::string& url, const std::string& content)
{
	std::string result;
	CURL *curl = curl_easy_init();
	if (curl) {
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 120L);
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 60L);
		curl_easy_setopt(curl, CURLOPT_HEADER, 0);

		struct curl_slist *headers = NULL;
		headers = curl_slist_append(headers, "Accept:application/json");
		headers = curl_slist_append(headers, "Content-Type:application/json");
		headers = curl_slist_append(headers, "charset:utf-8");

		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, content.c_str());
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HttpWriter);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

		CURLcode res = curl_easy_perform(curl);
		if (res != CURLE_OK)
			res = TryHttpOperation(url, curl, 3, 5);

		if (res != CURLE_OK) {
			Json::Value ret;
			ret["error"] = curl_easy_strerror(res);
			result = ret.toStyledString();
		}
		curl_easy_cleanup(curl);
		curl_slist_free_all(headers);
	}

	return result;
}

std::string HttpUtil::Put(const std::string& url, const std::string& content)
{
	std::string result;
	CURL *curl = curl_easy_init();
	if (curl) {
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 120L);
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 60L);
		curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, content.c_str());
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HttpWriter);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

		CURLcode res = curl_easy_perform(curl);
		if (res != CURLE_OK)
			res = TryHttpOperation(url, curl, 3, 5);

		if (res != CURLE_OK) {
			Json::Value ret;
			ret["error"] = curl_easy_strerror(res);
			result = ret.toStyledString();
		}
		curl_easy_cleanup(curl);
	}
	return result;
}

std::string HttpUtil::Delete(const std::string& url, const std::string& content)
{
	std::string result;
	CURL *curl = curl_easy_init();
	if (curl) {
		curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
		curl_easy_setopt(curl, CURLOPT_HEADER, 0);
		if (!content.empty())
			curl_easy_setopt(curl, CURLOPT_POSTFIELDS, content.c_str());
		curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HttpWriter);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

		CURLcode res = curl_easy_perform(curl);
		if (res != CURLE_OK)
			res = TryHttpOperation(url, curl, 3, 5);

		if (res != CURLE_OK) {
			Json::Value ret;
			ret["error"] = curl_easy_strerror(res);
			result = ret.toStyledString();
		}
		curl_easy_cleanup(curl);
	}
	return result;
}

std::string HttpUtil::ParseHttpURL(const std::string& target, size_t offset, URLParamMap& params)
{
	std::size_t pos = target.find('?');
	if (pos == std::string::npos)
		return target.substr(offset);
	std::string api_target = target.substr(offset, pos - offset);

	std::vector<std::string> p;
	boost::split(p, target.substr(pos + 1), boost::is_any_of("&"));
	for (auto& s : p) {
		pos = s.find('=');
		if (pos == std::string::npos)
			continue;
		params[s.substr(0, pos)] = s.substr(pos + 1);
	}
	return api_target;
}

}
