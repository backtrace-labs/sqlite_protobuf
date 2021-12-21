#include "protobuf_json.h"

#include <regex>
#include <string>

#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/dynamic_message.h>
#include <google/protobuf/util/json_util.h>

#include "sqlite3ext.h"

#include "utilities.h"

namespace sqlite_protobuf {
SQLITE_EXTENSION_INIT3

namespace {

using google::protobuf::Message;
using google::protobuf::util::JsonParseOptions;
using google::protobuf::util::JsonPrintOptions;
using google::protobuf::util::JsonStringToMessage;
using google::protobuf::util::MessageToJsonString;


/// Converts a binary blob of protobuf bytes to a JSON representation of the message.
///
///     SELECT protobuf_to_json(data, "Person");
///
/// @returns a JSON string.
void
protobuf_to_json(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    const std::string message_data = string_from_sqlite3_value(argv[0]);
    const std::string message_name = string_from_sqlite3_value(argv[1]);

    const Message* prototype = get_prototype(message_name);
    if (!prototype) {
        sqlite3_result_error(context, "Could not find message descriptor", -1);
        return;
    }

    std::unique_ptr<Message> message(prototype->New());
    if (!message->ParseFromString(message_data)) {
        sqlite3_result_error(context, "Failed to parse message", -1);
        return;
    }

    JsonPrintOptions options;
    // The JSON format is unfortunately tied to proto3 semantics,
    // where there is no difference between unpopulated primitive
    // fields and primitive fields set to their default value.  We may
    // parse this JSON in languages like C or Javascript that make it
    // easy to miss a null check, so we prefer to always populate
    // fields we know about.
    options.always_print_primitive_fields = true;

    std::string json;
    if (!MessageToJsonString(*message, &json, options).ok()) {
        sqlite3_result_error(context, "Could not convert message to JSON", -1);
        return;
    }

    sqlite3_result_text(context, json.c_str(), json.length(), SQLITE_TRANSIENT);
    return;
}

/// Converts a JSON string to a binary blob of protobuf bytes.
///
///     SELECT protobuf_of_json(json, "Person");
///
/// @returns a protobuf blob.
void
protobuf_of_json(sqlite3_context *context, int argc, sqlite3_value **argv)
{
    const std::string json_data = string_from_sqlite3_value(argv[0]);
    const std::string message_name = string_from_sqlite3_value(argv[1]);

    const Message *prototype = get_prototype(message_name);
    if (!prototype) {
        sqlite3_result_error(context, "Could not find message descriptor", -1);
        return;
    }

    std::unique_ptr<Message> message(prototype->New());

    JsonParseOptions options;
    options.ignore_unknown_fields = true;

    if (!JsonStringToMessage(json_data, message.get(), options).ok()) {
        sqlite3_result_error(context, "Could not parse JSON message", -1);
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
register_protobuf_json(sqlite3 *db, char **pzErrMsg,
    const sqlite3_api_routines *pApi)
{
    int rc;

    rc = sqlite3_create_function(db, "protobuf_to_json", 2,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC,
        nullptr, protobuf_to_json, nullptr, nullptr);
    if (rc != SQLITE_OK)
        return rc;

    return sqlite3_create_function(db, "protobuf_of_json", 2,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC,
        nullptr, protobuf_of_json, nullptr, nullptr);
}

}  // namespace sqlite_protobuf
