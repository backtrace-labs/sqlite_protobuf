#pragma once
#include <string>

#include <google/protobuf/message.h>

#include "sqlite3.h"

namespace sqlite_protobuf {

/// Convenience method for constructing a std::string from sqlite3_value
std::string string_from_sqlite3_value(sqlite3_value *value);

// Looks up a prototype message for the given `message_name`.  Returns
// the message on success and `nullptr` on failure.  The `context` is
// set into an error state on failure using `sqlite3_result_error`.
//
// The returned message may be reused across calls in the same thread,
// so should not be modified.  Ownership of the message is not passed
// to the caller.
const google::protobuf::Message* get_prototype(sqlite3_context *context,
                                               const std::string& message_name);
}  // namespace sqlite_protobuf
