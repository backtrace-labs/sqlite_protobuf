#pragma once
#include <string>

#include <google/protobuf/message.h>

#include "sqlite3.h"

namespace sqlite_protobuf {

/// Convenience method for constructing a std::string from sqlite3_value
std::string string_from_sqlite3_value(sqlite3_value *value);

/// Returns a prototype for the given message type, or nullptr if a descriptor
/// can't be found for the message type in the global generated descriptor pool.
/// The function uses the generated message factory singleton for prototype creation.
const google::protobuf::Message* get_prototype(const std::string& message_name);

}  // namespace sqlite_protobuf
