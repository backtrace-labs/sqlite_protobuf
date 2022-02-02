#include <proto_table.h>
#include <sqlite_protobuf.h>
#include <stdlib.h>

#include "test.pb-c.h"

static void
init_sqlite(void)
{
	int rc;

	rc = sqlite3_initialize();
	assert(rc == SQLITE_OK);

	rc = sqlite3_auto_extension((void (*)(void))sqlite3_sqliteprotobuf_init);
	assert(rc == SQLITE_OK);
}

static const struct proto_table T = {
	.name = "T",
	.message_name = "proto_table.TestMessage",
	.columns =
	    (struct proto_column[]) {
	    {
	        .name = "i64",
	        .type = "INTEGER",
	        .path = "$.i64",
	        .index = PROTO_SELECTOR_TYPE_STRONG,
	    },
	    {
	        .name = NULL,
	    },
	    },
};

static void
setup_db(sqlite3 *db)
{
	int rc;
	static char *cache;

	rc = proto_table_setup(&cache, db, &T);
	assert(rc == SQLITE_OK);
}

static void
exercise_bind_message(sqlite3 *db)
{
	int rc;
	int64_t id;
	sqlite3_stmt *stmt;
	ProtoTable__TestMessage *msg;

	/* Insert a test message. */
	rc = proto_prepare(
	    db, &stmt, "INSERT INTO T_raw(proto) VALUES (:proto) RETURNING id");
	assert(rc == SQLITE_OK);

	msg = malloc(sizeof *msg);
	proto_table__test_message__init(msg);

	msg->i64 = 17;

	PROTO_BIND(stmt, ":proto", &msg->base);

	rc = sqlite3_step(stmt);
	assert(rc == SQLITE_ROW);

	assert(sqlite3_column_count(stmt) == 1);

	id = sqlite3_column_int64(stmt, 0);
	assert(id != 0);

	proto_table__test_message__free_unpacked(msg, /*allocator=*/NULL);

	sqlite3_finalize(stmt);

	/* Fetch and decode the message we just inserted. */
	rc = proto_prepare(db, &stmt, "SELECT i64 FROM T WHERE id = (:id)");
	assert(rc == SQLITE_OK);

	PROTO_BIND(stmt, ":id", id);

	rc = sqlite3_step(stmt);
	assert(rc == SQLITE_ROW);

	assert(sqlite3_column_int64(stmt, 0) == 17);

	sqlite3_finalize(stmt);
}

int
main(void)
{
	int rc;
	sqlite3 *db;

	init_sqlite();

	rc = sqlite3_open(":memory:", &db);
	assert(rc == SQLITE_OK);

	setup_db(db);

	exercise_bind_message(db);

	sqlite3_close(db);

	return 0;
}
