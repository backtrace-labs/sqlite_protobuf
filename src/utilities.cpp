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

namespace {

struct cache {
    // Message type name or "".
    std::string message_name;

    // Cached prototype for the message type or `nullptr`.
    const Message *prototype;
};

inline struct cache *get_cache() {
    static thread_local struct cache cache;
    return &cache;
}

} // namespace

const Message *get_prototype(sqlite3_context *context,
                             const std::string& message_name)
{
    struct cache *cached = get_cache();

    // Look up the descriptor and prototype for the message type if necessary.
    if (cached->prototype == nullptr || cached->message_name != message_name) {
        cached->message_name = message_name;

        const DescriptorPool *pool = DescriptorPool::generated_pool();
        const Descriptor *descriptor = pool->FindMessageTypeByName(cached->message_name);
        if (descriptor) {
            MessageFactory *factory = MessageFactory::generated_factory();
            cached->prototype = factory->GetPrototype(descriptor);
        } else {
            sqlite3_result_error(context, "Could not find message descriptor", -1);

            // Invalidate the cache.
            cached->message_name.clear();
            cached->prototype = nullptr;
        }
    }

    return cached->prototype;
}

std::unique_ptr<Message> parse_message(sqlite3_context* context,
                                       const std::string& message_data,
                                       const std::string& message_name)
{
    const Message *prototype = get_prototype(context, message_name);
    if (!prototype)
        return nullptr;

    std::unique_ptr<Message> message(prototype->New());
    if (!message->ParseFromString(message_data)) {
        sqlite3_result_error(context, "Failed to parse message", -1);
        message.reset();
    }

    return message;
}

}  // namespace sqlite_protobuf
