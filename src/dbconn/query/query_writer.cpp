#include "dbconn/query/query_writer.hpp"

#include <cstdint>

namespace dbconnector {
namespace query {

std::string QueryWriter::EscapeQuotes(const std::string &text, char quote) {
	std::string result;
	for (auto c : text) {
		if (c == quote) {
			result += "\\";
			result += quote;
		} else if (c == '\\') {
			result += "\\\\";
		} else {
			result += c;
		}
	}
	return result;
}

std::string QueryWriter::WriteQuoted(const std::string &text, char quote) {
	// 1. Escapes all occurences of 'quote' by escaping them with a backslash
	// 2. Adds quotes around the string
	return std::string(1, quote) + EscapeQuotes(text, quote) + std::string(1, quote);
}

std::string QueryWriter::WriteIdentifier(const std::string &identifier, char identifier_quote) {
	return WriteQuoted(identifier, identifier_quote);
}

std::string QueryWriter::WriteLiteral(const std::string &identifier) {
	return QueryWriter::WriteQuoted(identifier, '\'');
}

std::string QueryWriter::EncodeBlob(const std::string &val) {
	char const HEX_DIGITS[] = "0123456789ABCDEF";

	std::string result = "x'";
	for (size_t i = 0; i < val.size(); i++) {
		uint8_t byte_val = static_cast<uint8_t>(val[i]);
		result += HEX_DIGITS[(byte_val >> 4) & 0xf];
		result += HEX_DIGITS[byte_val & 0xf];
	}
	result += "'";
	return result;
}

std::string QueryWriter::WriteConstant(const duckdb::Value &val) {
	if (val.type().IsNumeric() || val.type().id() == duckdb::LogicalTypeId::BOOLEAN) {
		return val.ToSQLString();
	}
	if (val.type().id() == duckdb::LogicalTypeId::BLOB) {
		return EncodeBlob(duckdb::StringValue::Get(val));
	}
	if (val.type().id() == duckdb::LogicalTypeId::TIMESTAMP_TZ) {
		return val.DefaultCastAs(duckdb::LogicalType::TIMESTAMP)
		    .DefaultCastAs(duckdb::LogicalType::VARCHAR)
		    .ToSQLString();
	}
	return val.DefaultCastAs(duckdb::LogicalType::VARCHAR).ToSQLString();
}

} // namespace query
} // namespace dbconnector
