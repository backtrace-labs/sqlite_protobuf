#pragma once

struct sqlite3;
struct sqlite3_api_routines;

namespace sqlite_protobuf {

int register_protobuf_enum(sqlite3 *db, char **pzErrMsg,
    const sqlite3_api_routines *pApi);

}  // namespace sqlite_protobuf
