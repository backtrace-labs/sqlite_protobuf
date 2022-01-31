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

static ProtoTable__TestMessage *
mkmsg(int64_t i64)
{
	ProtoTable__TestMessage *msg;

	msg = malloc(sizeof *msg);
	proto_table__test_message__init(msg);

	msg->i64 = i64;

	return msg;
}

static struct proto_result_row
get_row_by_id(sqlite3 *db, int64_t id)
{
	struct proto_result_row row;
	struct proto_result_list list;
	struct sqlite3_stmt *stmt;
	int rc;

	row = PROTO_RESULT_ROW_INITIALIZER;
	list = PROTO_RESULT_LIST_INITIALIZER;

	rc = proto_prepare(db, &stmt, "SELECT id, proto FROM T WHERE id = :id");
	assert(rc == SQLITE_OK);

	rc = PROTO_BIND(stmt, ":id", id);
	assert(rc == SQLITE_OK);

	rc = proto_result_list_populate(
	    &list, &proto_table__test_message__descriptor, db, stmt);
	assert(rc == SQLITE_OK);

	assert(list.count <= 1);

	if (list.count == 1) {
		row = list.rows[0];
		--list.count;
	}

	proto_result_list_reset(&list);

	sqlite3_finalize(stmt);

	return row;
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

	msg = mkmsg(17);

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

static void
exercise_proto_write_rows(sqlite3 *db)
{
	struct proto_result_list input_list;
	struct proto_result_list output_list;
	struct proto_result_row row;
	ProtoTable__TestMessage *msg;
	int64_t id;

	/*
	 * Insert a new row.
	 */
	input_list = PROTO_RESULT_LIST_INITIALIZER;
	output_list = PROTO_RESULT_LIST_INITIALIZER;

	msg = mkmsg(123);
	row = (struct proto_result_row) { .proto = &msg->base };
	proto_result_list_push_row(&input_list, &row);

	assert(proto_write_rows(db, &output_list, &input_list, "T") == SQLITE_OK);

	assert(input_list.count == 0);
	assert(output_list.count == 1);

	id = output_list.rows[0].id;
	assert(id != 0);

	/*
	 * Lookup the inserted row.
	 */
	row = get_row_by_id(db, id);
	assert(row.id == id);

	msg = (ProtoTable__TestMessage *)row.proto;
	assert(msg->i64 == 123);

	proto_result_list_push_row(&output_list, &row);

	/*
	 * Update the row.
	 */
	proto_result_list_reset(&input_list);

	msg = mkmsg(345);
	row = (struct proto_result_row) { .id = id, .proto = &msg->base };
	proto_result_list_push_row(&input_list, &row);

	assert(proto_write_rows(db, &output_list, &input_list, "T") == SQLITE_OK);

	/*
	 * Lookup the updated row.
	 */
	row = get_row_by_id(db, id);
	assert(row.id == id);

	msg = (ProtoTable__TestMessage *)row.proto;
	assert(msg->i64 == 345);

	proto_result_list_push_row(&output_list, &row);

	/* cleanup */
	proto_result_list_reset(&input_list);
	proto_result_list_reset(&output_list);
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

	exercise_proto_write_rows(db);

	sqlite3_close(db);

	return 0;
}
