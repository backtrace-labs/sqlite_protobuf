#include "utilities.h"

#include "sqlite3ext.h"

namespace sqlite_protobuf {
SQLITE_EXTENSION_INIT3

std::string string_from_sqlite3_value(sqlite3_value *value)
{
    return std::string(reinterpret_cast<const char*>(sqlite3_value_text(value)),
                        static_cast<size_t>(sqlite3_value_bytes(value)));
}

}  // namespace sqlite_protobuf
