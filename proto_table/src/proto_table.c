#include "proto_table.h"

#include <assert.h>
#include <inttypes.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <umash.h>

/*
 * Try to autocommit every `AUTOCOMMIT_BATCH_SIZE` write operation by
 * default.
 *
 * This value should be high enough to amortise the commit overhead
 * (fsync), but not so high that the write journal grows too large for
 * the page cache.  In practice, this means we should make it as small
 * as possible while preserving decent performance.
 */
#define AUTOCOMMIT_BATCH_SIZE 20000

static struct umash_params index_fp_params;

extern int proto_prepare(sqlite3 *, sqlite3_stmt **, const char *);

/**
 * We store the expression that corresponds to each of the view's
 * column in an array of these key-value structs.
 */
struct view_column {
	const char *column_name;
	char *expression;
	bool auto_index;
};

__attribute__((__constructor__)) static void
populate_fp_params(void)
{
	static const char key[32] = "proto table umash index fp key";

	umash_params_derive(&index_fp_params, 0, key);
	return;
}

/**
 * Generates a statement that will create an index on demand for
 * `index`, given the view column definitions in `columns`.
 */
static char *
create_index(char **OUT_index_name, const char *table_name,
    const struct view_column *columns, size_t num_columns,
    const struct proto_index *index, bool auto_index)
{
	char *index_name = NULL;
	char *index_expr;
	char *ret;

	*OUT_index_name = NULL;

	/* Construct the index expression. */
	index_expr = strdup("");
	if (index_expr == NULL)
		goto fail;

	for (size_t i = 0; index->components[i] != NULL; i++) {
		const char *component = index->components[i];
		const char *prefix = (i == 0) ? "\n  " : ",\n  ";
		char *update;

		/*
		 * See if we want to replace that with the view
		 * column's expansion.
		 */
		for (size_t j = 0; j < num_columns; j++) {
			if (strcmp(component, columns[j].column_name) == 0) {
				component = columns[j].expression;
				break;
			}
		}

		if (asprintf(&update, "%s%s%s", index_expr, prefix, component) < 0)
			goto fail;

		free(index_expr);
		index_expr = update;
	}

	/* Compute the index's name based on the index expression. */
	{
		struct umash_fp fp;

		fp = umash_fprint(
		    &index_fp_params, 0, index_expr, strlen(index_expr));
		if (asprintf(&index_name,
		    "proto_%sindex__%s__%s__%016" PRIx64 "%016" PRIx64,
		    (auto_index ? "auto" : ""), table_name, index->name_suffix,
		    fp.hash[0], fp.hash[1]) < 0) {
			index_name = NULL;
			goto fail;
		}
	}

	/*
	 * Re-use the old index if it already exists: we don't want to
	 * recreate it.
	 */
	if (asprintf(&ret,
	    "CREATE INDEX IF NOT EXISTS %s\n"
	    "ON %s_raw(%s\n);",
	    index_name, table_name, index_expr) < 0)
		goto fail;

	free(index_expr);
	*OUT_index_name = index_name;
	return ret;

fail:
	free(index_name);
	free(index_expr);
	return NULL;
}

/**
 * Returns a series of SQL statements to create the proto table's
 * backing raw table, as well as the view table, triggers, and
 * indexes on protopaths.
 *
 * The last statement is a `SELECT` statement that will list the name
 * of all indexes attached to the raw table that aren't necessary
 * anymore.
 */
static char *
generate_proto_table(const struct proto_table *table)
{
	char *create_raw = NULL;
	char *create_view = NULL;
	char *create_triggers = NULL;
	/* A list of column names, with a comma before each one. */
	char *column_names = NULL;
	/*
	 * A list of the corresponding CAST expressions, with a comma
	 * before each one.
	 */
	char *column_expressions = NULL;
	/*
	 * A comma-separated list of the index we want to create on
	 * the "raw" table.
	 */
	char *index_names = NULL;
	char *create_indexes = NULL;
	char *select_bad_indexes = NULL;
	struct view_column *view_columns = NULL;
	size_t num_view_columns = 0;
	char *ret = NULL;

	/*
	 * Make sure the raw table exists. No-op if there's already a
	 * raw table: we don't want to drop all that data.
	 */
	if (asprintf(&create_raw,
	    "CREATE TABLE IF NOT EXISTS %s_raw (\n"
	    "  id INTEGER PRIMARY KEY ASC NOT NULL,\n"
	    "  proto BLOB NOT NULL"
	    ");",
	    table->name) < 0) {
		create_raw = NULL;
		goto fail;
	}

	for (num_view_columns = 0;
	     table->columns != NULL && table->columns[num_view_columns].name != NULL;
	     num_view_columns++)
		;

	view_columns = calloc(num_view_columns, sizeof(view_columns[0]));
	column_names = strdup("");
	column_expressions = strdup("");

	if (column_names == NULL || column_expressions == NULL)
		goto fail;

	for (size_t i = 0; i < num_view_columns; i++) {
		const struct proto_column *column = &table->columns[i];
		struct view_column *view = &view_columns[i];
		char *update;

		view->column_name = column->name;
		if (asprintf(&view->expression,
		    "CAST(protobuf_extract(proto, '%s', '%s', NULL) AS %s)",
		    table->message_name, column->path, column->type) < 0) {
			view->expression = NULL;
			goto fail;
		}

		/* Weak selectors don't get auto indexes. */
		switch (column->index) {
		case PROTO_SELECTOR_TYPE_STRONG:
			view->auto_index = true;
			break;

		case PROTO_SELECTOR_TYPE_WEAK:
			view->auto_index = false;
			break;

		default:
			/* Index when in doubt. */
			view->auto_index = true;
			break;
		}

		/*
		 * The simple string concatenation here is
		 * quadratic-time and involves a lot of heap allocation,
		 * but you have bigger problems with your schemas if
		 * that's an issue.
		 */
		if (asprintf(&update, "%s,\n  %s", column_names, column->name) < 0)
			goto fail;
		free(column_names);
		column_names = update;

		if (asprintf(&update, "%s,\n  %s", column_expressions,
		    view->expression) < 0)
			goto fail;
		free(column_expressions);
		column_expressions = update;
	}

	/*
	 * Re-recreate our view: it's ok to drop the old view if any,
	 * since it doesn't hold any data.
	 */
	if (asprintf(&create_view,
	    "DROP VIEW IF EXISTS %s;\n"
	    "CREATE VIEW %s (\n"
	    "  id,\n"
	    "  proto%s\n"
	    ") AS SELECT\n"
	    "  id,\n"
	    "  proto%s\n"
	    "FROM %s_raw;",
	    table->name, table->name, column_names, column_expressions,
	    table->name) < 0) {
		create_view = NULL;
		goto fail;
	}

	/*
	 * Same thing for the triggers that map mutations on the view
	 * to mutation of the underlying raw table.
	 */
	if (asprintf(&create_triggers,
	    "DROP TRIGGER IF EXISTS %1$s_insert;\n"
	    "CREATE TRIGGER %1$s_insert INSTEAD OF INSERT ON %1$s\n"
	    "BEGIN\n"
	    "  INSERT INTO %1$s_raw(proto) VALUES(NEW.proto);\n"
	    "END;\n"
	    "DROP TRIGGER IF EXISTS %1$s_update;\n"
	    "CREATE TRIGGER %1$s_update INSTEAD OF UPDATE OF proto ON %1$s\n"
	    "BEGIN\n"
	    "  UPDATE %1$s_raw SET proto = NEW.proto WHERE id = OLD.id;\n"
	    "END;\n"
	    "DROP TRIGGER IF EXISTS %1$s_delete;\n"
	    "CREATE TRIGGER %1$s_delete INSTEAD OF DELETE ON %1$s\n"
	    "BEGIN\n"
	    "  DELETE FROM %1$s_raw WHERE id = OLD.id;\n"
	    "END;",
	    table->name) < 0) {
		create_triggers = NULL;
		goto fail;
	}

	/* Add an index for each view column. */
	create_indexes = strdup("");
	for (size_t i = 0; i < num_view_columns; i++) {
		const struct proto_index index = {
			.name_suffix = view_columns[i].column_name,
			.components =
			    (const char *[]) {
			    view_columns[i].column_name,
			    NULL,
			    },
		};
		char *index_name, *stmt, *update;
		int r;

		if (view_columns[i].auto_index == false)
			continue;

		stmt = create_index(&index_name, table->name, view_columns,
		    num_view_columns, &index, /*auto_index=*/true);
		if (stmt == NULL)
			goto fail;

#define HANDLE_INDEX()                                                              \
	do {                                                                        \
		if (index_names == NULL) {                                          \
			r = asprintf(&update, "'%s'", index_name);                  \
		} else {                                                            \
			r = asprintf(&update, "%s, '%s'", index_names, index_name); \
		}                                                                   \
                                                                                    \
		free(index_name);                                                   \
		if (r < 0) {                                                        \
			free(stmt);                                                 \
			goto fail;                                                  \
		}                                                                   \
                                                                                    \
		free(index_names);                                                  \
		index_names = update;                                               \
                                                                                    \
		r = asprintf(&update, "%s\n%s", create_indexes, stmt);              \
		free(stmt);                                                         \
		if (r < 0)                                                          \
			goto fail;                                                  \
                                                                                    \
		free(create_indexes);                                               \
		create_indexes = update;                                            \
	} while (0)

		HANDLE_INDEX();
	}

	/* And now add any additional index. */
	for (size_t i = 0;
	     table->indexes != NULL && table->indexes[i].name_suffix != NULL; i++) {
		char *index_name, *stmt, *update;
		int r;

		stmt = create_index(&index_name, table->name, view_columns,
		    num_view_columns, &table->indexes[i], /*auto_index=*/false);
		if (stmt == NULL)
			goto fail;

		HANDLE_INDEX();

#undef HANDLE_INDEX
	}

	/*
	 * List all `proto_index__` and `proto_autoindex__` indexes
	 * associated with the underlying raw table that we wouldn't
	 * have generated ourselves: we have to get rid of them, in
	 * case they refer to now-unknown fields in the protobuf
	 * message.
	 */
	if (asprintf(&select_bad_indexes,
	    "SELECT name FROM sqlite_master WHERE\n"
	    "  type = 'index' AND tbl_name = '%s_raw' AND\n"
	    "  (name LIKE 'proto_index__%%' OR name LIKE 'proto_autoindex__%%') AND\n"
	    "  name NOT IN (%s);",
	    table->name, index_names) < 0) {
		select_bad_indexes = NULL;
		goto fail;
	}

	if (asprintf(&ret,
	    "BEGIN EXCLUSIVE TRANSACTION;\n"
	    "%s\n%s\n\n%s\n%s\n"
	    "COMMIT TRANSACTION;\n"
	    "\n%s",
	    create_raw, create_view, create_triggers, create_indexes,
	    select_bad_indexes) < 0) {
		ret = NULL;
		goto fail;
	}

out:
	free(create_raw);
	free(create_view);
	free(create_triggers);
	free(column_names);
	free(column_expressions);
	free(index_names);
	free(create_indexes);

	for (size_t i = 0; i < num_view_columns; i++) {
		free(view_columns[i].expression);
	}

	free(view_columns);
	free(select_bad_indexes);
	return ret;

fail:
	free(ret);
	ret = NULL;
	goto out;
}

/**
 * We accumulate the name of indexes we want to get rid of in this
 * struct.
 */
struct bad_indexes {
	char **names;
	size_t num_names;
	bool log_to_stderr;
};

static int
bad_indexes_callback(void *thunk, int nargs, char **values, char **columns)
{
	struct bad_indexes *bad_indexes = thunk;
	char **new_names;
	char *name;

	(void)columns;
	if (nargs != 1)
		return 0;

	if (bad_indexes->log_to_stderr == true)
		fprintf(stderr, "Found unwanted proto_index: %s\n", values[0]);

	name = strdup(values[0]);
	if (name == NULL)
		return -1;

	new_names = realloc(bad_indexes->names,
	    (bad_indexes->num_names + 1) * sizeof(bad_indexes->names[0]));
	if (new_names == NULL) {
		free(name);
		return -1;
	}

	bad_indexes->names = new_names;
	bad_indexes->names[bad_indexes->num_names++] = name;
	return 0;
}

/**
 * Ensures the `spec`ced table in `db` is in the expected state.
 *
 * If the `command_cache` pointee is NULL, the pointer will be
 * populated with a cached SQL string that corresponds to `spec`; if
 * its pointee is non-NULL, it must have been generated by a prior
 * call to `setup_proto_table` for the same `spec`.
 */
int
proto_table_setup(char **command_cache, sqlite3 *db, const struct proto_table *spec)
{
	struct bad_indexes bad_indexes = {
		.log_to_stderr = spec->log_sql_to_stderr,
	};
	char *error;
	int rc;

	if (*command_cache == NULL) {
		*command_cache = generate_proto_table(spec);
		if (*command_cache == NULL)
			return SQLITE_NOMEM;

		if (spec->log_sql_to_stderr == true) {
			fprintf(stderr, "proto_index SQL for %s:\n%s\n", spec->name,
			    *command_cache);
		}
	}

	rc = sqlite3_exec(
	    db, *command_cache, bad_indexes_callback, &bad_indexes, &error);
	if (rc != SQLITE_OK)
		goto fail_sqlite;

	for (size_t i = 0; i < bad_indexes.num_names; i++) {
		char *remove_bad_index;

		if (asprintf(&remove_bad_index, "DROP INDEX IF EXISTS \"%s\";",
		    bad_indexes.names[i]) < 0) {
			rc = SQLITE_NOMEM;
			goto out;
		}

		rc = sqlite3_exec(db, remove_bad_index, NULL, NULL, &error);
		free(remove_bad_index);
		if (rc != SQLITE_OK)
			goto fail_sqlite;
	}

out:
	for (size_t i = 0; i < bad_indexes.num_names; i++)
		free(bad_indexes.names[i]);
	free(bad_indexes.names);
	return rc;

fail_sqlite:
	fprintf(stderr, "sqlite_exec failed for table %s: %s, rc=%i\n", spec->name,
	    error ?: "unknown error", rc);
	sqlite3_free(error);
	goto out;
}

static int
serialize_proto(void **OUT_bytes, size_t *OUT_count, const ProtobufCMessage *proto)
{
	void *buf;
	size_t actual;
	size_t capacity;

	*OUT_bytes = NULL;
	*OUT_count = 0;

	if (proto == NULL)
		return SQLITE_OK;

	capacity = protobuf_c_message_get_packed_size(proto);
	buf = malloc(capacity);
	if (buf == NULL)
		return SQLITE_NOMEM;

	actual = protobuf_c_message_pack(proto, buf);
	assert(actual <= capacity && "protobuf overflow");

	*OUT_bytes = buf;
	*OUT_count = actual;

	return SQLITE_OK;
}

int
proto_bind_helper_proto(sqlite3_stmt *stmt, int idx, const ProtobufCMessage *proto)
{
	void *bytes;
	size_t count;
	int rc;

	rc = serialize_proto(&bytes, &count, proto);
	if (rc)
		return rc;

	return sqlite3_bind_blob64(stmt, idx, bytes, count, free);
}

int64_t
proto_table_paginate(sqlite3 *db, const char *table, int64_t begin, size_t wanted)
{
	char *template;
	sqlite3_stmt *stmt = NULL;
	int64_t ret;
	int rc;

	if (asprintf(&template,
	    " SELECT COALESCE(MAX(id), :begin)"
	    " FROM ("
	    "   SELECT id"
	    "   FROM `%s`"
	    "   WHERE id > :begin"
	    "   ORDER BY id ASC"
	    "   LIMIT :wanted"
	    " )",
	    table) < 0) {
		return -(int64_t)SQLITE_NOMEM;
	}

	rc = proto_prepare(db, &stmt, template);
	if (rc != SQLITE_OK) {
		fprintf(
		    stderr, "failed to prepare pagination statement. rc=%i\n", rc);
		ret = -(int64_t)rc;
		goto out;
	}

	if ((rc = PROTO_BIND(stmt, ":begin", begin)) != SQLITE_OK ||
	    (rc = PROTO_BIND(stmt, ":wanted", wanted)) != SQLITE_OK) {
		fprintf(stderr, "failed to bind pagination parameters. rc=%i\n", rc);
		ret = -(int64_t)rc;
		goto out;
	}

	rc = sqlite3_step(stmt);
	switch (rc) {
	case SQLITE_DONE:
		ret = begin;
		break;

	case SQLITE_ROW:
		ret = sqlite3_column_int64(stmt, 0);
		break;

	default:
		fprintf(stderr, "failed to execute pagination query. rc=%i\n", rc);
		ret = -(int64_t)rc;
		break;
	}

out:
	(void)sqlite3_finalize(stmt);
	free(template);
	return ret;
}

int
proto_db_transaction_begin(struct proto_db *db)
{
	char *err = NULL;
	int rc;

	if (db->transaction_depth++ > 0) {
		assert(db->transaction_depth > 0);
		return SQLITE_OK;
	}

	rc = sqlite3_exec(db->db, "BEGIN IMMEDIATE TRANSACTION;", NULL, NULL, &err);
	if (rc != 0) {
		db->transaction_depth--;
		fprintf(stderr, "failed to open sqlite transaction, rc=%i: %s\n", rc,
		    err ?: "unknown error");
	}

	sqlite3_free(err);
	return rc;
}

void
proto_db_transaction_end(struct proto_db *db)
{
	char *err = NULL;
	int rc;

	assert(db->transaction_depth > 0);
	if (--db->transaction_depth > 0) {
		/* Cycle if we now can. */
		proto_db_count_writes(db, 0);
		return;
	}

	db->write_count = 0;
	rc = sqlite3_exec(db->db, "COMMIT TRANSACTION;", NULL, NULL, &err);
	if (rc != 0) {
		fprintf(stderr, "failed to commit sqlite transaction, rc=%i: %s\n",
		    rc, err ?: "unknown error");
		abort();
	}

	sqlite3_free(err);
	return;
}

int
proto_db_batch_begin(struct proto_db *db)
{

	db->autocommit_depth++;
	assert(db->autocommit_depth > 0);
	return proto_db_transaction_begin(db);
}

void
proto_db_batch_end(struct proto_db *db)
{

	proto_db_transaction_end(db);
	assert(db->autocommit_depth > 0);
	db->autocommit_depth--;
	return;
}

void
proto_db_count_writes(struct proto_db *db, size_t n)
{
	char *err = NULL;
	uint32_t batch_size = db->batch_size ?: AUTOCOMMIT_BATCH_SIZE;
	int rc;

	if (db->transaction_depth == 0)
		return;

	if (db->write_count < batch_size && n < batch_size - db->write_count) {
		db->write_count += n;
		return;
	}

	db->write_count = batch_size;
	if (db->autocommit_depth < db->transaction_depth)
		return;

	/*
	 * We want to and can flush writes.  Close the current
	 * transaction and immediately open a new one.
	 */
	db->write_count = 0;
	rc = sqlite3_exec(db->db, "COMMIT TRANSACTION; BEGIN IMMEDIATE TRANSACTION;",
	    NULL, NULL, &err);
	if (rc != 0) {
		fprintf(stderr, "failed to cycle sqlite transaction rc=%i: %s\n", rc,
		    err ?: "unknown error");
		/*
		 * If we failed to cycle the transaction, it's really
		 * not clear how the caller can recover.
		 */
		abort();
	}

	sqlite3_free(err);
	return;
}

void
proto_result_row_reset(struct proto_result_row *row)
{
	protobuf_c_message_free_unpacked(row->proto, /*allocator=*/NULL);
	free(row->bytes);

	*row = PROTO_RESULT_ROW_INITIALIZER;
	return;
}

void
proto_result_list_reset(struct proto_result_list *list)
{
	for (size_t i = 0; i < list->count; i++)
		proto_result_row_reset(&list->rows[i]);

	free(list->rows);

	*list = PROTO_RESULT_LIST_INITIALIZER;
	return;
}

static bool
result_list_grow(struct proto_result_list *dst, size_t increase)
{
	size_t want_capacity;

	want_capacity = dst->count + increase;

	if (want_capacity < increase)
		return false; /* overflow */

	if (want_capacity > SSIZE_MAX / sizeof(dst->rows[0]))
		return false; /* can never fit */

	while (dst->capacity < want_capacity) {
		struct proto_result_row *grown;
		size_t current = dst->capacity;
		size_t goal = 2 * current;

		if (goal < 8)
			goal = 8;

		if (goal < current || goal > SSIZE_MAX / sizeof(grown[0]))
			return false;

		grown = realloc(dst->rows, goal * sizeof(grown[0]));
		if (grown == NULL)
			return false;

		dst->rows = grown;
		dst->capacity = goal;
	}

	return true;
}

bool
proto_result_list_push_row(
    struct proto_result_list *dst, struct proto_result_row *row)
{

	if (dst->count >= dst->capacity &&
	    result_list_grow(dst, /*increase=*/1) == false)
		return false;

	dst->rows[dst->count++] = *row;
	*row = PROTO_RESULT_ROW_INITIALIZER;

	return true;
}

bool
proto_result_list_push(struct proto_result_list *dst, int64_t row,
    ProtobufCMessage *proto, void *bytes, size_t n_bytes)
{

	return proto_result_list_push_row(dst,
	    &(struct proto_result_row) {
	        .id = row,
	        .proto = proto,
	        .bytes = bytes,
	        .n_bytes = n_bytes,
	    });
}

int
proto_result_list_populate(struct proto_result_list *dst,
    const ProtobufCMessageDescriptor *type, sqlite3 *db, sqlite3_stmt *stmt)
{

	while (true) {
		int64_t row_id;
		void *copy; /* of the blob bytes. */
		void *parsed;
		const void *blob;
		size_t blob_size;
		int rc;

		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE)
			return SQLITE_OK;

		if (rc != SQLITE_ROW)
			return rc;

		row_id = sqlite3_column_int64(stmt, 0);
		blob = sqlite3_column_blob(stmt, 1);
		switch (sqlite3_errcode(db)) {
		case SQLITE_OK:
		case SQLITE_ROW:
			blob_size = (size_t)sqlite3_column_bytes(stmt, 1);
			break;

		case SQLITE_RANGE:
			blob = NULL;
			blob_size = 0;
			break;

		default:
			return sqlite3_errcode(db);
		}

		if (blob == NULL) {
			parsed = NULL;
			copy = NULL;
			blob_size = 0;
		} else {
			if (type != NULL) {
				parsed = protobuf_c_message_unpack(type,
				    /*allocator=*/NULL, blob_size, blob);
				if (parsed == NULL)
					return SQLITE_ROW;
			} else {
				parsed = NULL;
			}

			copy = NULL;
			/* Allocate one more to append a NUL terminator. */
			if (blob_size < SIZE_MAX)
				copy = malloc(blob_size + 1);

			if (copy == NULL) {
				protobuf_c_message_free_unpacked(
				    parsed, /*allocator=*/NULL);
				return SQLITE_NOMEM;
			}

			memcpy(copy, blob, blob_size);
			((uint8_t *)copy)[blob_size] = '\0';
		}

		if (proto_result_list_push(dst, row_id, parsed, copy, blob_size) ==
		    false) {
			protobuf_c_message_free_unpacked(parsed, /*allocator=*/NULL);
			free(copy);
			return SQLITE_NOMEM;
		}
	}
}

struct row_writer {
	sqlite3 *db;
	sqlite3_stmt *insert_stmt;
	sqlite3_stmt *update_stmt;
	const char *table_name;
};

static void
row_writer_reset(struct row_writer *self)
{
	sqlite3_finalize(self->insert_stmt);
	sqlite3_finalize(self->update_stmt);
	memset(self, 0, sizeof *self);
}

static int
row_writer_bind_proto_helper(sqlite3_stmt *stmt, struct proto_result_row *row)
{
	int rc;

	if (row->bytes == NULL) {
		rc =
		    serialize_proto((void **)&row->bytes, &row->n_bytes, row->proto);
		if (rc != SQLITE_OK)
			return rc;
	}

	return PROTO_BIND(stmt, ":proto",
	    (struct proto_bind_blob) {
	        .bytes = row->bytes,
	        .count = row->n_bytes,
	    });
}

static int
row_writer_insert(struct row_writer *self, struct proto_result_row *row)
{
	int rc;
	sqlite3_stmt *stmt;

	assert(row->id == 0);

	stmt = self->insert_stmt;
	if (stmt == NULL) {
		char *sql;

		/*
		 * We INSERT into the _raw table because our triggers
		 * on the cooked table cause `RETURNING` to not work.
		 */
		if (asprintf(&sql,
		    " INSERT INTO %s_raw(proto)"
		    " VALUES (:proto)"
		    " RETURNING id",
		    self->table_name) < 0) {
			return SQLITE_NOMEM;
		}

		rc = proto_prepare(self->db, &stmt, sql);
		free(sql);

		if (rc != SQLITE_OK)
			return rc;

		self->insert_stmt = stmt;
	}

	rc = row_writer_bind_proto_helper(stmt, row);
	if (rc != SQLITE_OK)
		return rc;

	rc = sqlite3_step(stmt);
	if (rc != SQLITE_ROW)
		return rc;

	assert(sqlite3_column_count(stmt) == 1);

	row->id = sqlite3_column_int64(stmt, 0);

	return sqlite3_reset(stmt);
}

static int
row_writer_update(struct row_writer *self, struct proto_result_row *row)
{
	int rc;
	sqlite3_stmt *stmt;

	assert(row->id != 0);

	stmt = self->update_stmt;
	if (stmt == NULL) {
		char *sql;

		if (asprintf(&sql,
		    " UPDATE %s"
		    " SET proto = :proto"
		    " WHERE id = :id",
		    self->table_name) < 0) {
			return SQLITE_NOMEM;
		}

		rc = proto_prepare(self->db, &stmt, sql);
		free(sql);

		if (rc != SQLITE_OK)
			return rc;

		self->update_stmt = stmt;
	}

	rc = row_writer_bind_proto_helper(stmt, row);
	if (rc != SQLITE_OK)
		return rc;

	rc = PROTO_BIND(stmt, ":id", row->id);
	if (rc != SQLITE_OK)
		return rc;

	rc = sqlite3_step(stmt);
	if (rc != SQLITE_DONE)
		return rc;

	return sqlite3_reset(stmt);
}

static int
row_writer_upsert(struct row_writer *self, struct proto_result_row *row)
{
	if (row->id == 0) {
		return row_writer_insert(self, row);
	} else {
		return row_writer_update(self, row);
	}
}

int
proto_write_rows(sqlite3 *db, struct proto_result_list *output_list,
    struct proto_result_list *input_list, const char *table_name)
{
	struct row_writer row_writer;
	size_t num_done = 0;
	size_t num_left = input_list->count;
	int rc;

	row_writer = (struct row_writer) {
		.db = db,
		.table_name = table_name,
	};

	/*
	 * Preallocate space on the output list so that transferring
	 * ownership always succeeds.
	 */
	if (result_list_grow(output_list, num_left) == false) {
		rc = SQLITE_NOMEM;
		goto out;
	}

	/* Insert or update each row as appropriate. */
	while (num_left > 0) {
		struct proto_result_row *row = &input_list->rows[num_done];

		rc = row_writer_upsert(&row_writer, row);
		if (rc != SQLITE_OK)
			goto out;

		if (proto_result_list_push_row(output_list, row) == false) {
			/* This cannot fail due to preallocation above. */
			abort();
		}

		num_done++;
		--num_left;
	}

out:
	/* Remove the done rows from the start of the input list. */
	memmove(&input_list->rows[0], &input_list->rows[num_done],
	    num_left * sizeof(input_list->rows[0]));

	memset(
	    &input_list->rows[num_left], 0, num_done * sizeof(input_list->rows[0]));

	input_list->count -= num_done;

	row_writer_reset(&row_writer);

	return rc;
}

int
proto_write_row(sqlite3 *db, struct proto_result_row *row, char const *table_name)
{
	struct proto_result_list input = PROTO_RESULT_LIST_INITIALIZER;
	struct proto_result_list output = PROTO_RESULT_LIST_INITIALIZER;
	int rc;

	if (proto_result_list_push_row(&input, row) == false)
		return SQLITE_NOMEM;

	rc = proto_write_rows(db, &output, &input, table_name);

	assert(input.count == 0 || input.count == 1);
	assert(output.count == 0 || output.count == 1);
	assert(input.count + output.count == 1);

	if (input.count == 1) {
		*row = input.rows[0];
		input.count = 0;
	}

	if (output.count == 1) {
		*row = output.rows[0];
		output.count = 0;
	}

	proto_result_list_reset(&input);
	proto_result_list_reset(&output);

	return rc;
}
