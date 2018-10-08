#ifndef FALCON_SERVER_BASE_H
#define FALCON_SERVER_BASE_H

#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include "HttpUtil.h"

namespace falcon {

namespace http = boost::beast::http;

class ServerBase
{
public:
	virtual ~ServerBase() { };

	virtual bool StartServer() = 0;

	virtual void RunServer() = 0;

	virtual int StopServer() = 0;

	virtual const char* GetName() = 0;

	bool IsStopped() const { return is_stopped.load(); }

protected:
	ServerBase() : is_stopped(false) { };

	std::atomic<bool> is_stopped;
};

template <typename ServerType>
struct Handler
{
	virtual ~Handler() { }
	virtual std::string Get(ServerType* server, const std::string& remote, std::string target, const URLParamMap& params, http::status& status)
	{
		status = http::status::bad_request;
		return "Illegal request-target";
	}
	virtual std::string Post(ServerType* server, const std::string& remote, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		status = http::status::bad_request;
		return "Illegal request-target";
	}
	virtual std::string Delete(ServerType* server, const std::string& remote, std::string target, const URLParamMap& params, const std::string& body, http::status& status)
	{
		status = http::status::bad_request;
		return "Illegal request-target";
	}
};

}

#endif
