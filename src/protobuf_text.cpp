#include "protobuf_text.h"

#include <regex>
#include <string>

#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/text_format.h>

#include "sqlite3ext.h"

#include "utilities.h"

namespace sqlite_protobuf {
SQLITE_EXTENSION_INIT3

namespace {

using google::protobuf::Message;
using google::protobuf::TextFormat;


/// Converts a binary blob of protobuf bytes to text proto.
///
///     SELECT protobuf_to_text(data, "Person");
///
/// @returns a text proto string.
void
protobuf_to_text(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    sqlite3_value *message_data = argv[0];
    sqlite3_value *message_name = argv[1];

    auto message = parse_message(context, message_data, message_name);
    if (!message) {
        return;
    }

    std::string text;
    if (!TextFormat::PrintToString(*message, &text)) {
        sqlite3_result_error(context, "Could not convert message to textproto", -1);
        return;
    }

    sqlite3_result_text(context, text.c_str(), text.length(), SQLITE_TRANSIENT);
    return;
}

/// Converts a text proto string to a binary blob of protobuf bytes.
///
///     SELECT protobuf_of_text(text_proto, "Person");
///
/// @returns a protobuf blob.
void
protobuf_of_text(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    const std::string text_data = string_from_sqlite3_value(argv[0]);
    sqlite3_value *message_name = argv[1];

    const Message *prototype = get_prototype(context, message_name);
    if (!prototype) {
        return;
    }

    std::unique_ptr<Message> message(prototype->New());
    if (!TextFormat::ParseFromString(text_data, message.get())) {
        sqlite3_result_error(context, "Could not parse text proto", -1);
        return;
    }

    std::string proto;
    if (!message->SerializeToString(&proto)) {
        sqlite3_result_error(context, "Could not serialize message", -1);
        return;
    }

    sqlite3_result_blob(context, proto.c_str(), proto.length(), SQLITE_TRANSIENT);
    return;
}

}  // namespace

int
register_protobuf_text(sqlite3 *db, char **pzErrMsg,
    const sqlite3_api_routines *pApi)
{
    int rc;

    rc = sqlite3_create_function(db, "protobuf_to_text", 2,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC,
        nullptr, protobuf_to_text, nullptr, nullptr);
    if (rc != SQLITE_OK)
        return rc;

    return sqlite3_create_function(db, "protobuf_of_text", 2,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC,
        nullptr, protobuf_of_text, nullptr, nullptr);
}

}  // namespace sqlite_protobuf
