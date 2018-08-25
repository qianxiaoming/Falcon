#ifndef FALCON_SERVER_BASE_H
#define FALCON_SERVER_BASE_H

namespace falcon {

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

}

#endif
