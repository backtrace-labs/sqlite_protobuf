#pragma once
#include <string>

#include "sqlite3.h"

namespace sqlite_protobuf {

/// Convenience method for constructing a std::string from sqlite3_value
std::string string_from_sqlite3_value(sqlite3_value *value);

}  // namespace sqlite_protobuf
