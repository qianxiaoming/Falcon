#ifndef FALCON_UTIL_H
#define FALCON_UTIL_H

#include <string>

namespace falcon {

class Util
{
public:
	static std::string GetModulePath()
	{
		char module_name[256] = { 0 };
		::GetModuleFileNameA(NULL, module_name, 256);
		if (char* pos = strrchr(module_name, '\\')) {
			*pos = 0;
			return module_name;
		}
		return "";
	}
};

}

#endif
