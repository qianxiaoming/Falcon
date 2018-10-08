#include <thread>
#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <curl/curl.h>
#include <json/json.h>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include <boost/format.hpp>
#include <boost/scoped_array.hpp>
#include "HttpUtil.h"

namespace falcon {

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
		curl_slist_free_all(headers);
		curl_easy_cleanup(curl);
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
		curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HttpWriter);
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

		struct curl_slist *headers = NULL;
		if (!content.empty()) {
			headers = curl_slist_append(headers, "Accept:application/json");
			headers = curl_slist_append(headers, "Content-Type:application/json");
			headers = curl_slist_append(headers, "charset:utf-8");
			curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
			curl_easy_setopt(curl, CURLOPT_POSTFIELDS, content.c_str());
		}

		CURLcode res = curl_easy_perform(curl);
		if (res != CURLE_OK)
			res = TryHttpOperation(url, curl, 3, 5);

		if (res != CURLE_OK) {
			Json::Value ret;
			ret["error"] = curl_easy_strerror(res);
			result = ret.toStyledString();
		}
		curl_slist_free_all(headers);
		curl_easy_cleanup(curl);
	}
	return result;
}

bool HttpUtil::UploadFile(const std::string& url, const std::string& file, uintmax_t file_size, uintmax_t chunk)
{
	Json::Value ret;
	boost::system::error_code ec;
	if (file_size == 0) {
		file_size = boost::filesystem::file_size(file, ec);
		if (ec) {
			LOG(ERROR) << ec.message();
			return false;
		}
	}
	uintmax_t count = file_size / chunk;
	if (file_size % chunk != 0)
		count++;
	uintmax_t offset = 0;
	FILE* f = fopen(file.c_str(), "rb");
	if (!f) {
		LOG(ERROR) << "Cannot open file " << file;
		return false;
	}

	CURL *curl = curl_easy_init();
	struct curl_slist *headers = NULL;
	headers = curl_slist_append(headers, "Accept:application/octet-stream");
	headers = curl_slist_append(headers, "Content-Type:application/octet-stream");
	headers = curl_slist_append(headers, "charset:utf-8");

	boost::scoped_array<char> buffer(new char[chunk]);
	for (uintmax_t i = 0; i < count; i++) {
		uintmax_t upload_size = i == (count - 1) ? (file_size % chunk) : chunk;
		if (upload_size == 0)
			upload_size = chunk;
		memset(buffer.get(), 0, chunk);
		fread(buffer.get(), 1, upload_size, f);

		std::string cur_url = url;
		cur_url += boost::str(boost::format("&offset=%ld&size=%ld") % offset % upload_size);
		curl_easy_setopt(curl, CURLOPT_URL, cur_url.c_str());
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPALIVE, 1L);
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPIDLE, 120L);
		curl_easy_setopt(curl, CURLOPT_TCP_KEEPINTVL, 60L);
		curl_easy_setopt(curl, CURLOPT_HEADER, 0);

		curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
		curl_easy_setopt(curl, CURLOPT_POSTFIELDS, buffer.get());
		curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, upload_size);
		curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, HttpWriter);
	
		std::string result;
		curl_easy_setopt(curl, CURLOPT_WRITEDATA, &result);

		CURLcode res = curl_easy_perform(curl);
		if (res != CURLE_OK)
			res = TryHttpOperation(url, curl, 3, 5);

		if (res != CURLE_OK) {
			LOG(ERROR) << curl_easy_strerror(res);
			break;
		}
		Json::Value value;
		std::istringstream iss(result);
		Json::CharReaderBuilder builder;
		builder["collectComments"] = false;
		if (!Json::parseFromStream(builder, iss, &value, NULL)) {
			LOG(ERROR) << result;
			return false;
		}
		if (value.isMember("error")) {
			LOG(ERROR) << value["error"].asString();
			return false;
		}
		offset += upload_size;
	}
	curl_easy_cleanup(curl);
	curl_slist_free_all(headers);
	fclose(f);
	return offset == file_size;
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
