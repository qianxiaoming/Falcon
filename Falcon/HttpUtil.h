#ifndef FALCON_HTTP_UTIL_H
#define FALCON_HTTP_UTIL_H

#include <string>
#include <json/json.h>

namespace falcon {

typedef std::map<std::string, std::string> URLParamMap;

class HttpUtil
{
public:
	static std::string Get(const std::string& url, char* ret_head = NULL);

	static std::string Post(const std::string& url, const std::string& content);

	static std::string Put(const std::string& url, const std::string& content);

	static std::string Delete(const std::string& url, const std::string& content = "");

	static bool UploadFile(const std::string& url, const std::string& file, uintmax_t file_size = 0, uintmax_t chunk=1024*1024);

	static std::string HttpUtil::ParseHttpURL(const std::string& target, size_t offset, URLParamMap& params);
};

}

#endif
