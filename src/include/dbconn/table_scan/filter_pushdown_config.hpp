#pragma once

#include <string>

namespace dbconnector {
namespace table_scan {

struct FilterPushdownConfig {
	char identifier_quote;

	explicit FilterPushdownConfig(char identifier_quote_p) : identifier_quote(identifier_quote_p) {
	}
};

} // namespace table_scan
} // namespace dbconnector
