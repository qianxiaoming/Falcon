#include "stdafx.h"
#include <iostream>
#include "libfalcon.h"

int main(int argc, char* argv[])
{
	using namespace falcon;
	using namespace falcon::api;

	int port = MASTER_CLIENT_PORT;
	if (argc > 2)
		port = std::atoi(argv[2]);
	ComputingCluster cluster(argv[1], port);
	if (!cluster.IsConnected()) {
		std::cerr << "Failed to connect to " << argv[1] << std::endl;
		return EXIT_FAILURE;
	}
	std::cout << "==========" << cluster.GetName() << "==========" << std::endl;
	return EXIT_SUCCESS;
}
