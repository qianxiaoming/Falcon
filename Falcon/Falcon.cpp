#include "Falcon.h"

namespace falcon {

const char* ToString(Job::Type type)
{
	switch (type) {
	case Job::Type::Batch:
		return "Batch";
	case Job::Type::DAG:
		return "DAG";
	default:
		assert(false);
	}
	return "";
}

const char* ToString(Job::State state)
{
	switch (state) {
	case Job::State::Queued:
		return "Queued";
	case Job::State::Waiting:
		return "Waiting";
	case Job::State::Executing:
		return "Executing";
	case Job::State::Halted:
		return "Halted";
	case Job::State::Completed:
		return "Completed";
	case Job::State::Failed:
		return "Failed";
	case Job::State::Terminated:
		return "Terminated";
	default:
		assert(false);
	}
	return "";
}

}