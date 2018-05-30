#include <stdio.h>
#include <stdlib.h>
#include <tchar.h>
#define WIN32_LEAN_AND_MEAN 
#include <Windows.h>
#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include "MasterServer.h"
#include "Util.h"

SERVICE_STATUS ServiceStatus;
SERVICE_STATUS_HANDLE hServiceStatusHandle;
void WINAPI ServiceMain(int argc, char** argv);
void WINAPI ServiceHandler(DWORD fdwControl);

static falcon::ServerBase* server_base = nullptr;

DWORD WINAPI ServiceCoreThread(LPVOID para)
{
	if (!server_base->StartServer())
		return EXIT_FAILURE;
	server_base->RunServer();
	return EXIT_SUCCESS;
}


void WINAPI ServiceHandler(DWORD fdwControl)
{
	switch (fdwControl)
	{
	case SERVICE_CONTROL_STOP:
	case SERVICE_CONTROL_SHUTDOWN:
	{
		int retn = server_base->StopServer();
		Sleep(3000);

		ServiceStatus.dwWin32ExitCode = retn;
		ServiceStatus.dwCurrentState = SERVICE_STOPPED;
		ServiceStatus.dwCheckPoint = 0;
		ServiceStatus.dwWaitHint = 0;
		LOG(INFO) << "Falcon stopped";
		google::ShutdownGoogleLogging();
		break;
	}
	default:
		return;
	};
	SetServiceStatus(hServiceStatusHandle, &ServiceStatus);
}


void WINAPI ServiceMain(int argc, char** argv)
{
	FLAGS_log_dir = falcon::Util::GetModulePath();
	google::InitGoogleLogging(server_base->GetName());

	ServiceStatus.dwServiceType = SERVICE_WIN32;
	ServiceStatus.dwCurrentState = SERVICE_START_PENDING;
	ServiceStatus.dwControlsAccepted = SERVICE_ACCEPT_STOP | SERVICE_ACCEPT_SHUTDOWN | SERVICE_ACCEPT_PAUSE_CONTINUE;
	ServiceStatus.dwWin32ExitCode = 0;
	ServiceStatus.dwServiceSpecificExitCode = 0;
	ServiceStatus.dwCheckPoint = 0;
	ServiceStatus.dwWaitHint = 0;
	hServiceStatusHandle = RegisterServiceCtrlHandler(server_base->GetName(), ServiceHandler);
	if (hServiceStatusHandle == 0)
		LOG(ERROR) << "Failed to register service handler: " << GetLastError();

	HANDLE task_handle = CreateThread(NULL, NULL, ServiceCoreThread, NULL, NULL, NULL);
	if (task_handle == NULL)
		LOG(ERROR)<<"Create service thread failed: "<<GetLastError();

	ServiceStatus.dwCurrentState = SERVICE_RUNNING;
	ServiceStatus.dwCheckPoint = 0;
	ServiceStatus.dwWaitHint = 9000;
	if (!SetServiceStatus(hServiceStatusHandle, &ServiceStatus))
		LOG(ERROR) << "Failed to update service status: " << GetLastError();
	LOG(INFO) << "Falcon started";
}

int main(int argc, const char *argv[])
{
	if (strcmp(argv[1], "master") == 0)
		server_base = falcon::MasterServer::Instance();

	SERVICE_TABLE_ENTRY ServiceTable[2];
	ServiceTable[0].lpServiceName = (LPSTR)server_base->GetName();
	ServiceTable[0].lpServiceProc = (LPSERVICE_MAIN_FUNCTION)ServiceMain;
	ServiceTable[1].lpServiceName = NULL;
	ServiceTable[1].lpServiceProc = NULL;

	StartServiceCtrlDispatcher(ServiceTable);
	return 0;
}

//int main(int argc, const char *argv[])
//{
//	char module_name[256] = { 0 };
//	::GetModuleFileNameA(NULL, module_name, 256);
//	if (char* pos = strrchr(module_name, '\\')) {
//		*pos = 0;
//		FLAGS_log_dir = module_name;
//	}
//	google::InitGoogleLogging("lts-master");
//
//	if (!falcon::StartMasterService())
//		return EXIT_FAILURE;
//	else {
//		std::thread service_thread(falcon::RunMasterService);
//		service_thread.detach();
//	}
//
//	getchar();
//	falcon::StopMasterService();
//	Sleep(3000);
//
//	LOG(INFO) << "Lightweight task scheduler master service stopped";
//	google::ShutdownGoogleLogging();
//	return 0;
//}