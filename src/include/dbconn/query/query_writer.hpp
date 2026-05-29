#pragma once

#include <string>

#include "duckdb/common/types/value.hpp"

namespace dbconnector {
namespace query {

class QueryWriter {

public:
	static std::string WriteIdentifier(const std::string &identifier, char identifier_quote);
	static std::string WriteLiteral(const std::string &identifier);
	static std::string WriteConstant(const duckdb::Value &val);

private:
	static std::string EncodeBlob(const std::string &val);
	static std::string EscapeQuotes(const std::string &text, char quote);
	static std::string WriteQuoted(const std::string &text, char quote);
};

} // namespace query
} // namespace dbconnector
