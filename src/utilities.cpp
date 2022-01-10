#include <atomic>
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
    const char *text = static_cast<const char*>(sqlite3_value_blob(value));
    size_t text_size = static_cast<size_t>(sqlite3_value_bytes(value));
    return std::string(text, text_size);
}

namespace {

bool string_equal_to_sqlite3_value(const std::string& str, sqlite3_value *val)
{
    // Allow any conversion to take place first.
    const char *text = static_cast<const char *>(sqlite3_value_blob(val));

    size_t text_size = static_cast<size_t>(sqlite3_value_bytes(val));
    if (text_size != str.size())
        return false;

    if (!text)
        return false;

    return memcmp(text, str.data(), text_size) == 0;
}

/*
 * Whenever `protobuf_load` completes loading a shared object we
 * need to invalidate the prototype and message caches of all threads.
 * This is done by having `invalidate_all_caches` increment a global
 * generation counter atomically, and then `get_prototype` checking
 * against a previously saved value in its cache.
 */
std::atomic<uint64_t> global_prototype_generation;

/*
 * We attempt to reuse `Message` objects for parsing by clearing them
 * to reduce memory allocation overheads.  To avoid constantly
 * reallocating when there are really small messages mixed in with
 * moderately sized messages, we consider messages smaller than this
 * limit to actually have this size.
 */
const size_t MIN_MESSAGE_DATA_REUSE_SIZE = 256;

struct cache {
    // Message type name or "".
    std::string message_name;

    // Cached prototype for the message type or `nullptr`.
    const Message *prototype;

    // Encoded message data of cached message or "".
    std::string message_data;

    // The cached result of parsing of the `message_data` or `nullptr`.
    std::unique_ptr<Message> message;

    // The maximum size of the encoded message we have parsed
    // using `message`. Used to reset `message` if the size of
    // the encoded messages drops suddenly.
    size_t max_message_data_size;

    // This is compared to the global counter before checking
    // for a match against `message_name`.
    uint64_t prototype_generation;
};

inline struct cache *get_cache() {
    static thread_local struct cache cache;
    return &cache;
}

void invalidate_message_cache()
{
    struct cache *cached = get_cache();
    cached->message_data.clear();
    cached->message.reset();
    cached->max_message_data_size = 0;
}

void invalidate_prototype_cache()
{
    struct cache *cached = get_cache();
    cached->message_name.clear();
    cached->prototype = nullptr;
    cached->prototype_generation = global_prototype_generation.load(std::memory_order_acquire);
    invalidate_message_cache();
}

} // namespace

void invalidate_all_caches()
{
    global_prototype_generation.fetch_add(1, std::memory_order_acq_rel);
}

const Message *get_prototype(sqlite3_context *context,
                             sqlite3_value *message_name)
{
    struct cache *cached = get_cache();

    // Look up the descriptor and prototype for the message type if necessary.
    uint64_t global_gen = global_prototype_generation.load(std::memory_order_acquire);
    if (global_gen != cached->prototype_generation ||
        cached->prototype == nullptr ||
        !string_equal_to_sqlite3_value(cached->message_name, message_name)) {

        cached->message_name = string_from_sqlite3_value(message_name);

        const DescriptorPool *pool = DescriptorPool::generated_pool();
        const Descriptor *descriptor = pool->FindMessageTypeByName(cached->message_name);
        if (descriptor) {
            MessageFactory *factory = MessageFactory::generated_factory();
            cached->prototype = factory->GetPrototype(descriptor);
            cached->prototype_generation = global_gen;
            invalidate_message_cache();
        } else {
            sqlite3_result_error(context, "Could not find message descriptor", -1);
            invalidate_prototype_cache();
        }
    }

    return cached->prototype;
}

Message *parse_message(sqlite3_context* context,
                       sqlite3_value *message_data,
                       sqlite3_value *message_name)
{
    // Lookup the prototype.
    const Message *prototype = get_prototype(context, message_name);
    if (!prototype)
        return nullptr;

    struct cache *cached = get_cache();

    // Parse the message if we haven't already.
    if (cached->message != nullptr &&
        string_equal_to_sqlite3_value(cached->message_data,message_data)) {
        return cached->message.get();
    }

    cached->message_data = string_from_sqlite3_value(message_data);

    // Make sure we have an empty Message object to parse with.  Reuse
    // an existing Message object if the size of the message to parse
    // doesn't shrink too much.
    size_t cmp_size = std::max(cached->message_data.size(),
                               MIN_MESSAGE_DATA_REUSE_SIZE);
    if (cached->message && cmp_size >= cached->max_message_data_size/2) {
        cached->message->Clear();
    } else {
        cached->message.reset(prototype->New());
        cached->max_message_data_size = 0;
    }

    cached->max_message_data_size = std::max(cached->max_message_data_size,
                                             cached->message_data.size());

    if (!cached->message->ParseFromString(cached->message_data)) {
        sqlite3_result_error(context, "Failed to parse message", -1);
        invalidate_message_cache();
    }

    return cached->message.get();
}

}  // namespace sqlite_protobuf
