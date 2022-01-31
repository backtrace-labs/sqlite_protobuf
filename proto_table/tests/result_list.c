#include <proto_table.h>
#include <stdlib.h>

static ProtobufCMessage *nullproto = NULL;

static struct proto_result_row
mkrow(int64_t id, ProtobufCMessage *proto, const void *bytes, size_t n_bytes)
{
	struct proto_result_row row = PROTO_RESULT_ROW_INITIALIZER;
	row.id = id;
	row.proto = proto;
	row.bytes = malloc(n_bytes);
	row.n_bytes = n_bytes;
	memcpy(row.bytes, bytes, n_bytes);
	return row;
}

static void
assert_row_eq(struct proto_result_row a, struct proto_result_row b)
{
	assert(a.id == b.id);
	assert(a.proto == b.proto);
	assert(a.bytes == b.bytes);
	assert(a.n_bytes == b.n_bytes);
}

static void
assert_row_is_initialized(struct proto_result_row row)
{
	assert_row_eq(row, PROTO_RESULT_ROW_INITIALIZER);
}

static void
exercise_push(void)
{
	struct proto_result_list list;
	struct proto_result_row row, orig;

	list = PROTO_RESULT_LIST_INITIALIZER;

	/* push_row */
	orig = mkrow(123, nullproto, "yoyoyo", 6);
	row = orig;

	assert(proto_result_list_push_row(&list, &row) == true);

	assert(list.count == 1);
	assert(list.capacity >= list.count);

	assert_row_is_initialized(row);
	assert_row_eq(list.rows[0], orig);

	/* just push */
	row = mkrow(256, nullproto, "bob dole", 8);
	assert(proto_result_list_push(
	       &list, row.id, row.proto, row.bytes, row.n_bytes) == true);

	assert(list.count == 2);
	assert(list.capacity >= list.count);

	assert_row_eq(list.rows[1], row);

	/* cleanup */
	proto_result_list_reset(&list);

	assert(list.count == 0);
	assert(list.capacity == 0);
	assert(list.rows == NULL);
}

static void
exercise_growth(void)
{
	struct proto_result_list list;
	size_t initial_capacity;
	size_t num_growths_limit;
	size_t num_growths_seen;
	size_t num_rows;

	list = PROTO_RESULT_LIST_INITIALIZER;
	initial_capacity = 0;
	num_growths_limit = 3;
	num_growths_seen = 0;
	num_rows = 0;

	/*
	 * Add rows until we see a number of discrete jumps in
	 * capacity.
	 */
	while (num_growths_seen < num_growths_limit) {
		int64_t id;
		struct proto_result_row row;
		size_t capacity_before;

		capacity_before = list.capacity;

		id = ++num_rows;
		row = mkrow(id, nullproto, "abc", 3);

		assert(proto_result_list_push_row(&list, &row) == true);

		/*
		 * Witness the initial allocation and then each growth
		 * step in capacity after.
		 */
		if (initial_capacity == 0)
			initial_capacity = list.capacity;
		else if (list.capacity > capacity_before)
			++num_growths_seen;
	}

	assert(list.count == num_rows);

	/*
	 * Whitebox tests the growth rate. Checks that capacity at
	 * least doubles after the initial allocation.
	 */
	assert(num_rows > (initial_capacity << (num_growths_seen - 1)));

	proto_result_list_reset(&list);
}

static void
run_all(void)
{
	exercise_push();
	exercise_growth();
}

int
main(void)
{

	run_all();

	return 0;
}
