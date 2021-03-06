#pragma once

#include <assert.h>
#include <protobuf-c/protobuf-c.h>
#include <sqlite3.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

/**
 * A proto table is a view over a raw sqlite table that only contains two
 * columns: a integer primary key id, and a proto blob of protobuf bytes.
 *
 * The proto index constructs a view where columns correspond to
 * values extracted from the each row's protobuf blob, with indexes
 * to ensure reasonable query performance.
 */

/**
 * A `proto_column` describes one view column obtained by extracting a
 * protobuf path from the underlying raw table.
 */
struct proto_column {
	/* The name of the view column. */
	const char *name;

	/*
	 * The sqlite type of the column data (e.g, TEXT or INTEGER).
	 *
	 * The `protobuf_extract` function is opaque to the query
	 * planner, so we provide explicit type information by
	 * `CAST`ing around calls to `protobuf_extract`.
	 */
	const char *type;

	/* The protopath to pass to `protobuf_extract`. */
	const char *path;

	enum proto_selector_type {
		/*
		 * A strong selector (the default) gets an automatic
		 * index on its value.
		 */
		PROTO_SELECTOR_TYPE_STRONG = 0,
		/*
		 * A column that's weak will not be used to select
		 * rows by itself (i.e., it's a weak selector).  It
		 * will not be indexed automatically, but may appear
		 * in explicit indexes.
		 *
		 * It's easier to remove an index than to add one, and
		 * the only impact of letting a column be strong when
		 * it should be weak is a constant factor slowdown on
		 * inserts and updates.  When in doubt, use the
		 * default (strong) until there's a good reason to
		 * avoid indexing a column.
		 */
		PROTO_SELECTOR_TYPE_WEAK = 1,
	} index;
};

/**
 * A `proto_index` describes an additional index added to the raw
 * table.
 *
 * Each view column automatically gets an index on its expression.
 */
struct proto_index {
	/*
	 * Unique name for the index.
	 */
	const char *name_suffix;
	/*
	 * NULL terminated list of column names; any component that is
	 * not a view column name is passed verbatim as an index
	 * expression.
	 */
	const char *const *components;
};

/**
 * A `proto_table` describes a view built on top of a "raw" table
 * (`${name}_raw`) that only has two columns: an integer primary key
 * `id`, and a blob of protobuf bytes, `proto`.
 *
 * From that raw table, we construct a view that extract values from
 * each row's proto blob; we make that's reasonably efficient with
 * functional indexes.
 */
struct proto_table {
	/*
	 * The name of the view table.  We will construct a prefix for
	 * the underlying raw table and all indexes.
	 */
	const char *name;

	/*
	 * Whether to log the proto table's setup SQL to stderr
	 * whenever `proto_table_setup` constructs that SQL string
	 * from scratch.
	 */
	bool log_sql_to_stderr;

	/*
	 * The protobuf message type for all rows in this raw table.
	 */
	const char *message_name;

	/*
	 * List of additional columns in the view, terminated by a
	 * zero-filled struct.
	 *
	 * We automatically generate an index for each protobuf
	 * columns (and for the integer primary key).
	 */
	const struct proto_column *columns;

	/*
	 * List of additional indexes on the raw table, terminated by
	 * a zero-filled struct.
	 */
	const struct proto_index *indexes;
};

/**
 * It's often easier to issue a lot of small writes when working with
 * protobuf, which makes transactions essential for write performance.
 * This struct type wraps a sqlite3 db with counters for transaction
 * depth and *autocommit* transaction depth; as long as all
 * transactions on the db are for autocommit, we can flush writes
 * whenever the write count is too high.
 *
 * This wrapper lets caller open multiple overlapping (not necessarily
 * nested) transactions for the same sqlite database: the real sqlite
 * transaction is only affected when transitioning between
 * transaction_depth 1 and 0.
 *
 * The semantics are particularly useful when a function uses
 * transactions for correctness, and is called from another function
 * that opens a transaction for performance.  However, it also means
 * that ROLLBACKs have a much wider blast radius than one might
 * expect.
 */
struct proto_db {
	/*
	 * Counts the number of write operations performed
	 * since the last transaction commit.
	 */
	uint32_t write_count;

	/*
	 * Attempt to autoflush after this many write operations.
	 *
	 * Defaults to a reasonable batch size if 0.
	 */
	uint32_t batch_size;

	/*
	 * Sqlite doesn't nest transactions, so we track the
	 * depth on our end.
	 */
	size_t transaction_depth;

	/*
	 * Counts the number of transactions that were only
	 * created for write batch performance.  When this
	 * number is equal to the transaction depth, we can
	 * cycle the current transaction to flush writes
	 * whenever we want.
	 */
	size_t autocommit_depth;

	sqlite3 *db;
};

/**
 * A `proto_result_row` struct represents a single result row for a
 * sqlite query.
 */
struct proto_result_row {
	int64_t id; /* row id */
	ProtobufCMessage *proto;
	void *bytes;
	size_t n_bytes;
};

struct proto_result_list {
	size_t count;
	size_t capacity;
	struct proto_result_row *rows;
};

#define PROTO_RESULT_LIST_INITIALIZER                                               \
	(struct proto_result_list) { .count = 0 }

#define PROTO_RESULT_LIST(LIST_TYPE, PROTO_TYPE, ROW_TYPE)                          \
	struct LIST_TYPE {                                                          \
		union {                                                             \
			struct {                                                    \
				size_t count;                                       \
				size_t capacity;                                    \
				struct ROW_TYPE {                                   \
					int64_t id;                                 \
					PROTO_TYPE *proto;                          \
					void *bytes;                                \
					size_t n_bytes;                             \
				} * rows;                                           \
			};                                                          \
			struct proto_result_list impl;                              \
		};                                                                  \
	}

/**
 * Ensures the `spec`ced table in `db` is in the expected state.
 *
 * If the `command_cache` pointee is NULL, the pointer will be
 * populated with a cached SQL string that corresponds to `spec`; if
 * its pointee is non-NULL, it must have been generated by a prior
 * call to `setup_proto_table` for the same `spec`.
 *
 * The sqlite_protobuf extension must be loaded before calling this
 * function, and the message descriptors must be present in the C++
 * protobuf repository before accessing the view.
 *
 * Returns 0 (SQLITE_OK) on success, and an sqlite error code on
 * failure.
 */
int proto_table_setup(
    char **command_cache, sqlite3 *db, const struct proto_table *spec);

/**
 * Find the end id for page of up to `wanted` rows in `table`,
 * starting at `id >= begin`.
 *
 * Returns `begin` when there is none, and a negated sqlite error code
 * on error.
 */
int64_t proto_table_paginate(
    sqlite3 *, const char *table, int64_t begin, size_t wanted);

/**
 * Attempts to open a new transaction in the proto db.  This wrapper
 * counts recursive invocation, and only opens a sqlite transaction
 * when the count transitions from 0 to 1.
 *
 * Rolling back is rarely a good idea using protodb.
 *
 * Returns SQLITE_OK on success, and a sqlite error code on failure.
 */
int proto_db_transaction_begin(struct proto_db *);

/**
 * Closes one transaction in the proto db.  This wrapper counts
 * recursive transactions (nested or otherwise), and only closes the
 * underlying sqlite transaction when the total count hits 0.
 *
 * Aborts on failure.
 */
void proto_db_transaction_end(struct proto_db *);

/**
 * Attempts to open a new autocommit transaction (only for
 * performance) in the proto db.
 *
 * Returns SQLITE_OK on success, and a sqlite error code on failure.
 */
int proto_db_batch_begin(struct proto_db *);

/**
 * Closes one autocommit transaction in the proto db.
 *
 * Aborts on failure.
 */
void proto_db_batch_end(struct proto_db *);

/**
 * Updates the proto db for `count` new writes operations (rows added
 * or modified).
 *
 * Aborts on transaction flush failure.
 */
void proto_db_count_writes(struct proto_db *, size_t count);

/**
 * Releases any resource owned by the list and reinitialises it to a
 * zero-filled struct.
 */
void proto_result_list_reset(struct proto_result_list *);

/**
 * Pushes results from the sqlite3 statement to the proto_result_list.
 *
 * The first result column must be an integer row id, and the second
 * column should be a blob.  If a descriptor is passed the blob is
 * also parsed as a protobuf message.
 *
 * Returns SQLITE_OK (0) on success and a sqlite error code on failure.
 *
 * When a descriptor is passed and we fail to parse a row's second
 * (blob) value as protobuf, fails with SQLITE_ROW.
 */
int proto_result_list_populate(struct proto_result_list *,
    const ProtobufCMessageDescriptor *, sqlite3 *, sqlite3_stmt *);

/**
 * Prepares a statement for the sqlite handle `db`, and stores the
 * result in `stmt` on success.
 */
inline int
proto_prepare(sqlite3 *db, sqlite3_stmt **stmt, const char *sql)
{

	return sqlite3_prepare_v2(db, sql, strlen(sql) + 1, stmt, NULL);
}

/**
 * The PROTO_BIND{,_INDEX} make it easier to bind values to sqlite
 * prepared statements.
 */

/**
 * This placeholder struct describes SQL NULL values.  A constant
 * named `sqlite_null` will have a value of type `struct
 * proto_bind_null` inside `PROTO_BIND`'s and `PROTO_BIND_INDEX`'s
 * evaluation scopes.
 */
struct proto_bind_null {
	char structs_must_have_some_field;
};

/**
 * A `proto_bind_blob` struct describes a binary blob value
 * of `count` bytes starting at `bytes`.
 */
struct proto_bind_blob {
	const void *bytes;
	size_t count;
};

/**
 * A `proto_bind_zeroblob` struct describes a zero-filled binary blob
 * of `count` bytes.
 */
struct proto_bind_zeroblob {
	size_t count;
};

/**
 * Binds the third value argument to the `IDX`th parameter in prepared
 * statement `STMT`.
 *
 * Pass `sqlite_null` to bind a null, and a `proto_bind_blob`
 * to bind a sized byte range.  C strings are bound as utf-8 strings,
 * integers as int64, and floating point values as double.
 *
 * When value is a pointer (`proto_bind_blob` or C strings), the pointee
 * must outlive the prepared statement, or at least the lifetime of the
 * current statement up to the next call to `sqlite3_reset` on it.
 */
#define PROTO_BIND_INDEX(STMT, IDX, ...)                                            \
	({                                                                          \
		sqlite3_stmt *const PBI_s = (STMT);                                 \
		const int PBI_i = (IDX);                                            \
		const struct proto_bind_null sqlite_null = { 0 };                   \
		__auto_type PBI_v = (__VA_ARGS__);                                  \
                                                                                    \
		(void)sqlite_null;                                                  \
		_Generic(PBI_v,						\
		    struct proto_bind_null: proto_bind_helper_null,	\
									\
		    char *: proto_bind_helper_cstring,			\
		    const char *: proto_bind_helper_cstring,		\
									\
		    struct proto_bind_blob: proto_bind_helper_blob,	\
		    struct proto_bind_zeroblob: proto_bind_helper_zeroblob,\
									\
		    const sqlite3_value *: sqlite3_bind_value,		\
		    sqlite3_value *: sqlite3_bind_value,		\
									\
		    int8_t: sqlite3_bind_int64, 			\
		    uint8_t: sqlite3_bind_int64,			\
		    int16_t: sqlite3_bind_int64,			\
		    uint16_t: sqlite3_bind_int64,			\
		    int32_t: sqlite3_bind_int64,			\
		    uint32_t: sqlite3_bind_int64,			\
		    int64_t: sqlite3_bind_int64,			\
		    uint64_t: sqlite3_bind_int64,			\
									\
		    float: sqlite3_bind_double,				\
		    double: sqlite3_bind_double)			\
			(PBI_s, PBI_i, PBI_v);                                              \
	})

/**
 * Binds parameter `NAME` in prepared statement `STMT` to the third
 * argument's value.  Asserts out if no such parameter can be found
 * in the prepared statement.
 *
 * See `SQLITE_BIND_INDEX` for the specific type dispatch on that
 * value.
 *
 * Values bound as strings or blobs must outlive the prepared
 * statement (or at least, the statement must be reset before
 * the values' lifetimes end).
 */
#define PROTO_BIND(STMT, NAME, ...)                                                 \
	({                                                                          \
		sqlite3_stmt *const PB_stmt = (STMT);                               \
		const char *const PB_name = (NAME);                                 \
		const int PB_i = sqlite3_bind_parameter_index(PB_stmt, PB_name);    \
                                                                                    \
		assert(PB_i != 0 && "parameter not found");                         \
		PROTO_BIND_INDEX(PB_stmt, PB_i, ##__VA_ARGS__);                     \
	})

static inline int
proto_bind_helper_null(sqlite3_stmt *stmt, int idx, struct proto_bind_null v)
{

	(void)v;
	return sqlite3_bind_null(stmt, idx);
}

static inline int
proto_bind_helper_cstring(sqlite3_stmt *stmt, int idx, const char *v)
{

	return sqlite3_bind_text64(
	    stmt, idx, v, strlen(v), SQLITE_STATIC, SQLITE_UTF8);
}

static inline int
proto_bind_helper_blob(sqlite3_stmt *stmt, int idx, struct proto_bind_blob v)
{

	return sqlite3_bind_blob64(stmt, idx, v.bytes, v.count, SQLITE_STATIC);
}

static inline int
proto_bind_helper_zeroblob(sqlite3_stmt *stmt, int idx, struct proto_bind_zeroblob v)
{

	return sqlite3_bind_zeroblob64(stmt, idx, v.count);
}
