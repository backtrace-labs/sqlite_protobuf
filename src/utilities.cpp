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

const Message *get_prototype(sqlite3_context *context,
                             const std::string& message_name)
{
    static thread_local struct {
        // Message type name or "".
        std::string message_name;

        // Cached prototype for the message type or `nullptr`.
        const Message *prototype;
    } cached;

    // Look up the descriptor and prototype for the message type if necessary.
    if (cached.prototype == nullptr || cached.message_name != message_name) {
        cached.message_name = message_name;

        const DescriptorPool *pool = DescriptorPool::generated_pool();
        const Descriptor *descriptor = pool->FindMessageTypeByName(cached.message_name);
        if (descriptor) {
            MessageFactory *factory = MessageFactory::generated_factory();
            cached.prototype = factory->GetPrototype(descriptor);
        } else {
            sqlite3_result_error(context, "Could not find message descriptor", -1);

            // Invalidate the cache.
            cached.message_name.clear();
            cached.prototype = nullptr;
        }
    }

    return cached.prototype;
}

}  // namespace sqlite_protobuf
