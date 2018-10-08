#ifndef FALCON_API_H
#define FALCON_API_H

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

#include <string>
#include <memory>
#include <list>
#include "CommonDef.h"

namespace falcon {
namespace api {

class Job;
typedef std::shared_ptr<Job> JobPtr;
typedef std::list<JobPtr> JobList;

class FALCON_API ComputingCluster
{
public:
	ComputingCluster(const std::string& server, uint16_t port);

	bool IsConnected() const;

	std::string GetName() const;

	std::string SubmitJob();

	bool TerminateJob(const std::string& id);

	JobPtr QueryJob(const std::string& id) const;

	JobList QueryJobList() const;

private:
	std::string server_addr;
	std::string cluster_name;
	bool        connected;
};

}
}

#endif
