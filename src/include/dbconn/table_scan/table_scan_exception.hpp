#pragma once

#include <stdexcept>

namespace dbconnector {
namespace table_scan {

class TableScanException : public std::exception {
protected:
	std::string message;

public:
	TableScanException() = default;

	TableScanException(const std::string &message) : message(message.data(), message.length()) {
	}

	virtual const char *what() const noexcept {
		return message.c_str();
	}
};

} // namespace table_scan
} // namespace dbconnector
