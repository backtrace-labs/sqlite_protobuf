#include "protobuf_extract.h"

#include <regex>
#include <string>

#include <google/protobuf/descriptor_database.h>
#include <google/protobuf/dynamic_message.h>

#include "sqlite3ext.h"

#include "utilities.h"

namespace sqlite_protobuf {
SQLITE_EXTENSION_INIT3

namespace {

using google::protobuf::Descriptor;
using google::protobuf::EnumDescriptor;
using google::protobuf::EnumValueDescriptor;
using google::protobuf::FieldDescriptor;
using google::protobuf::Message;
using google::protobuf::Reflection;


/// For enum fields, handle the special suffix paths .name and .number
static bool handle_special_enum_path(sqlite3_context *context,
                                     const EnumDescriptor *enum_descriptor,
                                     int value,
                                     const std::string& path,
                                     std::string::const_iterator& it)
{
    // Get the remainder of the path
    std::string rest = path.substr(std::distance(path.begin(), it));
    
    if (rest == "" || rest == ".number")
    {
        sqlite3_result_int64(context, value);
        return true;
    } 
    else if (rest == ".name")
    {
        const EnumValueDescriptor *value_descriptor =
            enum_descriptor->FindValueByNumber(value);
        if (!value_descriptor)
        {
            sqlite3_result_error(context, "Enum value not found", -1);
            return false;
        }
        
        sqlite3_result_text(context,
            value_descriptor->name().c_str(),
            value_descriptor->name().length(),
            SQLITE_TRANSIENT);
        return true;
    }
    
    // This error message should match what happens for non-enums also
    sqlite3_result_error(context, "Path traverses non-message elements", -1);
    return false;
}


/// Return the element (or elements) 
///
///     SELECT protobuf_extract(data, "Person", "$.phones[0].number", default?);
///
/// @returns a Protobuf-encoded BLOB or the appropriate SQL datatype
///
/// If `default` is provided, it is returned instead of the protobuf field's
/// default value.
static void protobuf_extract(sqlite3_context *context,
                             int argc,
                             sqlite3_value **argv)
{
    if (argc < 3 || argc > 4) {
        sqlite3_result_error(
            context,
            "wrong number of arguments to function protobuf_extract (expected 3 or 4)",
            SQLITE_CONSTRAINT_FUNCTION);
        return;
    }

    const std::string message_data = string_from_sqlite3_value(argv[0]);
    const std::string message_name = string_from_sqlite3_value(argv[1]);
    const std::string path = string_from_sqlite3_value(argv[2]);
    sqlite3_value *const default_value = (argc >= 4) ? argv[3] : nullptr;
    
    // Check that the path begins with $, representing the root of the tree
    if (path.length() == 0 || path[0] != '$') {
        sqlite3_result_error(context, "Invalid path", -1);
        return;
    }
    
    // Find the message prototype and descriptor.
    const Message *prototype = get_prototype(message_name);
    if (!prototype) {
        sqlite3_result_error(context, "Could not find message descriptor", -1);
        return;
    }

    // Deserialize the message
    std::unique_ptr<Message> root_message(prototype->New());
    if (!root_message->ParseFromString(message_data)) {
        sqlite3_result_error(context, "Failed to parse message", -1);
        return;
    }
    
    // Special case: just return the root object
    if (path == "$") {
        sqlite3_result_blob(context, message_data.c_str(),
            message_data.length(), SQLITE_TRANSIENT);
        return;
    }
    
    // Get the Descrptor interface for the message type
    const Descriptor* descriptor = prototype->GetDescriptor();

    // Get the Reflection interface for the message
    const Reflection *reflection = root_message->GetReflection();
    
    // As we traverse the tree, this is the "current" message we are looking at.
    // We only want the overall message to be managed by std::unique_ptr,
    // and this variable will always point into the overall structure.
    const Message *message = root_message.get();
    
    // Parse the rest
    static const auto& path_element_regex =
        *new std::regex("^\\.([^\\.\\[]+)(?:\\[(-?[0-9]+)\\])?");
    std::string::const_iterator it = ++ path.cbegin();  // skip $
    while (it != path.end()) {
        std::smatch m;
        if (!std::regex_search(it, path.cend(), m, path_element_regex)) {
            sqlite3_result_error(context, "Invalid path", -1);
            return;
        }
        
        // Advance the iterator to the start of the next path component
        it += m.length();
        
        const std::string field_name = m.str(1);
        const std::string field_index_str = m.str(2);
        
        // Get the descriptor for this field by its name
        const FieldDescriptor *const field =
            descriptor->FindFieldByName(field_name);
        if (!field) {
            sqlite3_result_error(context, "Invalid field name", -1);
            return;
        }

        // If the field is optional, and it is not provided, return the default
        if (field->is_optional() && !reflection->HasField(*message, field)) {
            // Is there anything left in the path?
            if (it != path.end()) {
                switch (field->type())
                {
                   case FieldDescriptor::Type::TYPE_ENUM:
                   case FieldDescriptor::Type::TYPE_MESSAGE:
                       // Both of these are handled in the main switch below
                       break;
                   default:
                       sqlite3_result_error(context, "Invalid path", -1);
                       return;
                }
            }

            if (default_value != nullptr) {
                sqlite3_result_value(context, default_value);
                return;
            }
            
            switch(field->cpp_type()) {
            case FieldDescriptor::CppType::CPPTYPE_INT32:
                sqlite3_result_int64(context, field->default_value_int32());
                return;
            case FieldDescriptor::CppType::CPPTYPE_INT64:
                sqlite3_result_int64(context, field->default_value_int64());
                return;
            case FieldDescriptor::CppType::CPPTYPE_UINT32:
                sqlite3_result_int64(context, field->default_value_uint32());
                return;
            case FieldDescriptor::CppType::CPPTYPE_UINT64:
                sqlite3_log(SQLITE_WARNING,
                    "Protobuf field \"%s\" is unsigned, but SQLite does not "
                    "support unsigned types", field->full_name().c_str());
                sqlite3_result_int64(context, field->default_value_uint64());
                return;
            case FieldDescriptor::CppType::CPPTYPE_DOUBLE:
                sqlite3_result_double(context, field->default_value_double());
                return;
            case FieldDescriptor::CppType::CPPTYPE_FLOAT:
                sqlite3_result_double(context, field->default_value_float());
                return;
            case FieldDescriptor::CppType::CPPTYPE_BOOL:
                sqlite3_result_int64(context,
                    field->default_value_bool() ? 0 : 1);
                return;
            case FieldDescriptor::CppType::CPPTYPE_ENUM:
                handle_special_enum_path(context,
                    field->default_value_enum()->type(),
                    field->default_value_enum()->number(),
                    path, it);
                return;
            case FieldDescriptor::CppType::CPPTYPE_STRING:
                switch(field->type()) {
                default:
                    // fall through, but log
                    sqlite3_log(SQLITE_WARNING,
                        "Protobuf field \"%s\" is an unexpected string type",
                        field->full_name().c_str());
                case FieldDescriptor::Type::TYPE_STRING:
                    sqlite3_result_text(context,
                        field->default_value_string().c_str(),
                        field->default_value_string().length(),
                        SQLITE_TRANSIENT);
                    break;
                case FieldDescriptor::Type::TYPE_BYTES:
                    sqlite3_result_blob(context,
                        field->default_value_string().c_str(),
                        field->default_value_string().length(),
                        SQLITE_TRANSIENT);
                    break;
                }
                return;
            case FieldDescriptor::CppType::CPPTYPE_MESSAGE:
                sqlite3_result_null(context);
                return;
            }
        }

        int field_index = 0;
        const bool is_repeated = field->is_repeated();
        // If the field is repeated, validate the index into it
        if (is_repeated) {
            if (field_index_str.empty()) {
                sqlite3_result_error(context,
                    "Expected index into repeated field", -1);
                return;
            }

            // Wrap around for negative indexing
            int field_size = reflection->FieldSize(*message, field);
            field_index = std::stoi(field_index_str);

            if (field_index < 0) {
                field_index = field_size + field_index;
            }

            // Check that it's within range
            if (field_index < 0 || field_index >= field_size) {
                // If we error here, that means the query will stop
                sqlite3_result_null(context);
                return;
            }
        }

        // If the field is a submessage, descend into it
        if (field->cpp_type() == FieldDescriptor::CppType::CPPTYPE_MESSAGE) {
            // Descend into this submessage
            message = is_repeated
                ? &reflection->GetRepeatedMessage(*message, field, field_index)
                : &reflection->GetMessage(*message, field);
            descriptor = message->GetDescriptor();
            reflection = message->GetReflection();
            continue;
        }
        
        // Any other type should be the end of the path
        if (it != path.cend()
            && field->type() != FieldDescriptor::Type::TYPE_ENUM)
        {
            sqlite3_result_error(context, "Path traverses non-message elements",
                -1);
            return;
        }
        
        // Translate the field type into a SQLite type and return it
        switch(field->cpp_type()) {
            case FieldDescriptor::CppType::CPPTYPE_INT32:
            {
                int32_t value = is_repeated
                    ? reflection->GetRepeatedInt32(*message, field, field_index)
                    : reflection->GetInt32(*message, field);
                sqlite3_result_int64(context, value);
                return;
            }
            case FieldDescriptor::CppType::CPPTYPE_INT64:
            {
                int64_t value = is_repeated
                    ? reflection->GetRepeatedInt64(*message, field, field_index)
                    : reflection->GetInt64(*message, field);
                sqlite3_result_int64(context, value);
                return;
            }
            case FieldDescriptor::CppType::CPPTYPE_UINT32:
            {
                uint32_t value = is_repeated
                    ? reflection->GetRepeatedUInt32(*message, field, field_index)
                    : reflection->GetUInt32(*message, field);
                sqlite3_result_int64(context, value);
                return;
            }
            case FieldDescriptor::CppType::CPPTYPE_UINT64:
            {
                sqlite3_log(SQLITE_WARNING,
                    "Protobuf field \"%s\" is unsigned, but SQLite does not "
                    "support unsigned types");
                uint64_t value = is_repeated
                    ? reflection->GetRepeatedUInt64(*message, field, field_index)
                    : reflection->GetUInt64(*message, field);
                sqlite3_result_int64(context, value);
                return;
            }
            case FieldDescriptor::CppType::CPPTYPE_DOUBLE:
            {
                double value = is_repeated
                    ? reflection->GetRepeatedDouble(*message, field, field_index)
                    : reflection->GetDouble(*message, field);
                sqlite3_result_double(context, value);
                return;
            }
            case FieldDescriptor::CppType::CPPTYPE_FLOAT:
            {
                float value = is_repeated
                    ? reflection->GetRepeatedFloat(*message, field, field_index)
                    : reflection->GetFloat(*message, field);
                sqlite3_result_double(context, value);
                return;
            }
            case FieldDescriptor::CppType::CPPTYPE_BOOL:
            {
                bool value = is_repeated
                    ? reflection->GetRepeatedBool(*message, field, field_index)
                    : reflection->GetBool(*message, field);
                sqlite3_result_int64(context, value ? 0 : 1);
                return;
            }
            case FieldDescriptor::CppType::CPPTYPE_ENUM:
            {
                int value = is_repeated
                    ? reflection->GetRepeatedEnumValue(*message, field, field_index)
                    : reflection->GetEnumValue(*message, field);
                handle_special_enum_path(context, field->enum_type(), value,
                    path, it);
                return;
            }
            case FieldDescriptor::CppType::CPPTYPE_STRING:
            {
                std::string value = is_repeated
                    ? reflection->GetRepeatedString(*message, field, field_index)
                    : reflection->GetString(*message, field);
                switch(field->type()) {
                default:
                    // fall through, but log
                    sqlite3_log(SQLITE_WARNING,
                        "Protobuf field \"%s\" is an unexpected string type",
                        field->full_name().c_str());
                case FieldDescriptor::Type::TYPE_STRING:
                    sqlite3_result_text(context, value.c_str(), value.length(),
                        SQLITE_TRANSIENT);
                    break;
                case FieldDescriptor::Type::TYPE_BYTES:
                    sqlite3_result_blob(context, value.c_str(), value.length(),
                        SQLITE_TRANSIENT);
                    break;
                }
                return;
            }
            case FieldDescriptor::CppType::CPPTYPE_MESSAGE:
                // Covered separately above, silence the warning
                break;
        }
    }
    
    // We made it to the end of the path. This means the user selected for a
    // message, which we should return the Protobuf-encoded message we landed on
    std::string serialized;
    if (!message->SerializeToString(&serialized)) {
        sqlite3_result_error(context, "Could not serialize message", -1);
        return;
    }
    sqlite3_result_blob(context, serialized.c_str(), serialized.length(),
        SQLITE_TRANSIENT);
}
}  // namespace

int
register_protobuf_extract(sqlite3 *db, char **pzErrMsg,
    const sqlite3_api_routines *pApi)
{
    return sqlite3_create_function(db, "protobuf_extract", -1,
        SQLITE_UTF8 | SQLITE_DETERMINISTIC, 0, protobuf_extract, 0, 0);
}

}  // namespace sqlite_protobuf
