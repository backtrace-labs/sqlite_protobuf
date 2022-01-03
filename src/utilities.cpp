#include "sqlite3ext.h"
#include "utilities.h"

using google::protobuf::Descriptor;
using google::protobuf::DescriptorPool;
using google::protobuf::MessageFactory;
using google::protobuf::Message;

namespace sqlite_protobuf {
SQLITE_EXTENSION_INIT3

std::string string_from_sqlite3_value(sqlite3_value *value)
{
    return std::string(reinterpret_cast<const char*>(sqlite3_value_text(value)),
                        static_cast<size_t>(sqlite3_value_bytes(value)));
}

const Message* get_prototype(const std::string &message_name) {
    static thread_local struct {
        std::string message_name;
        const Message* prototype;
    } cached;

    if (cached.prototype == nullptr || cached.message_name != message_name) {
        const Descriptor *descriptor = DescriptorPool::generated_pool()->FindMessageTypeByName(message_name);
        if (!descriptor) {
            return nullptr;
        }

        MessageFactory *const factory = MessageFactory::generated_factory();

        cached.message_name = message_name;
        cached.prototype = factory->GetPrototype(descriptor);
    }
    return cached.prototype;
}

}  // namespace sqlite_protobuf
