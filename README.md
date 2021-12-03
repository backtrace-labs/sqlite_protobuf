This fork of [sqlite\_protobuf](https://github.com/rgov/sqlite_protobuf)
fixes some issues (e.g.,
[#15](https://github.com/rgov/sqlite_protobuf/issues/15)) and removes the
test suite that we do not use.

It also comes with `proto_table`, a C library to construct ergonomic
views on top of the `sqlite_protobuf` extension.

# Protobuf Extension for SQLite

This project implements a [run-time loadable extension][ext] for
[SQLite][]. It allows SQLite to perform queries that can extract field
values out of stored Protobuf messages.

[ext]: https://www.sqlite.org/loadext.html
[SQLite]: https://www.sqlite.org/

See also the [JSON1 Extension][json1], which is a similar extension
for querying JSON-encoded database content.

[json1]: https://www.sqlite.org/json1.html
