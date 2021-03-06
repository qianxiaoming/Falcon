﻿#include <stdio.h>
#include <stdlib.h>
#include <tchar.h>
#define WIN32_LEAN_AND_MEAN 
#include <Windows.h>
#define GLOG_NO_ABBREVIATED_SEVERITIES
#include <glog/logging.h>
#include <boost/bind.hpp>
#include <boost/filesystem.hpp>
#include <boost/algorithm/string.hpp>
#include "Falcon.h"
#include "MasterServer.h"
#include "SlaveServer.h"
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
		LOG(INFO) << "Falcon service stopped";
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
	FLAGS_log_dir = falcon::Util::GetModulePath()+"/logs";
	FLAGS_logbuflevel = -1;
	boost::filesystem::create_directories(FLAGS_log_dir);
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
	LOG(INFO) << "Falcon service started";
}

int main(int argc, const char *argv[])
{
	if (strcmp(argv[1], "master") == 0)
		server_base = falcon::MasterServer::Instance();
	else if (strcmp(argv[1], "slave") == 0) {
		falcon::SlaveServer* slave = falcon::SlaveServer::Instance();
		if (argc >= 3)
			slave->SetSlavePort(std::atoi(argv[2]));
		else if (char* port = getenv("WIT3D_SLAVE_PORT"))
			slave->SetSlavePort(std::atoi(port));
		else
			slave->SetSlavePort(falcon::SLAVE_LISTEN_PORT);

		std::string config_file = falcon::Util::GetModulePath() + "/Wit3d-Slave.conf";
		if (argc >= 4)
			slave->SetMasterAddr(argv[3]);
		else if (char* addr = getenv("WIT3D_MASTER_IP"))
			slave->SetMasterAddr(addr);
		else if (boost::filesystem::exists(config_file)) {
			FILE* f = fopen(config_file.c_str(), "r");
			if (f) {
				char ip[128] = { 0 };
				fgets(ip, 128, f);
				fclose(f);
				char* eq = strchr(ip, '=');
				if (eq) {
					std::string master_ip = eq + 1;
					slave->SetMasterAddr(boost::trim_copy(master_ip));
				} else
					return EXIT_FAILURE;
			}
		} else
			slave->SetMasterAddr("127.0.0.1");
		server_base = slave;
	} else
		return EXIT_FAILURE;

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
//	std::string role = argv[1];
//	std::string logging = std::string("falcon-") + role;
//	FLAGS_log_dir = falcon::Util::GetModulePath() + "/logs";
//	FLAGS_logbuflevel = -1;
//	FLAGS_alsologtostderr = true;
//	boost::filesystem::create_directories(FLAGS_log_dir);
//	google::InitGoogleLogging(logging.c_str());
//
//	if (role == "slave") {
//		falcon::SlaveServer* slave_server = falcon::SlaveServer::Instance();
//		slave_server->SetSlavePort(argc > 2 ? std::atoi(argv[2]) : falcon::SLAVE_LISTEN_PORT);
//		slave_server->SetMasterAddr(argc > 3 ? argv[3] : "127.0.0.1");
//		if (!slave_server->StartServer())
//			return EXIT_FAILURE;
//		else {
//			std::thread service_thread(boost::bind(&falcon::SlaveServer::RunServer, slave_server));
//			service_thread.detach();
//		}
//
//		getchar();
//		slave_server->StopServer();
//		Sleep(3000);
//		falcon::SlaveServer::Destory();
//	} else {
//		std::unique_ptr<falcon::MasterServer> master_server(new falcon::MasterServer());
//		if (!master_server->StartServer())
//			return EXIT_FAILURE;
//		else {
//			std::thread service_thread(boost::bind(&falcon::MasterServer::RunServer, master_server.get()));
//			service_thread.detach();
//		}
//		
//		getchar();
//		master_server->StopServer();
//		Sleep(3000);
//		falcon::MasterServer::Destory();
//	}
//
//	google::ShutdownGoogleLogging();
//	return 0;
//}
