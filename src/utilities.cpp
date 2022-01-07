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

    // Encoded message data of cached message or "".
    std::string message_data;

    // The cached result of parsing of the `message_data`, if any.
    std::unique_ptr<Message> message;

    // The maximum size of the encoded message we have parsed
    // using `message`. Used to reset `message` if the size of
    // the encoded messages drops suddenly.
    size_t max_message_data_size;

    // True iff we have managed to parse `message_data`.
    bool parse_success;
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

            // Prime the message cache.
            cached->message.reset(cached->prototype->New());
            cached->parse_success = true;
        } else {
            sqlite3_result_error(context, "Could not find message descriptor", -1);

            // Invalidate the cache.
            cached->message_name.clear();
            cached->prototype = nullptr;
            cached->message.reset();
            cached->parse_success = false;
        }
        cached->message_data.clear();
        cached->max_message_data_size = 0;
    }

    return cached->prototype;
}

Message *parse_message(sqlite3_context* context,
                       const std::string& message_data,
                       const std::string& message_name)
{
    // Lookup the prototype.
    const Message *prototype = get_prototype(context, message_name);
    if (!prototype)
        return nullptr;

    struct cache *cached = get_cache();

    // Parse the message if we haven't already.
    if (cached->message_data != message_data) {
        cached->message_data = message_data;

        // Make sure we have an empty Message object to parse with.
        // Reuse an existing Message object if the size of the message
        // to parse doesn't shrink too much.
        if (cached->message_data.size() >= cached->max_message_data_size / 2) {
            cached->message->Clear();
        } else {
            cached->message.reset(prototype->New());
            cached->max_message_data_size = 0;
        }

        cached->max_message_data_size = std::max(cached->max_message_data_size,
                                                 cached->message_data.size());

        cached->parse_success = cached->message->ParseFromString(cached->message_data);
    }

    if (!cached->parse_success) {
        sqlite3_result_error(context, "Failed to parse message", -1);
        return nullptr;
    }

    return cached->message.get();
}

}  // namespace sqlite_protobuf
