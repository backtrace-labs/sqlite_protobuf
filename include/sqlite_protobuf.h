#pragma once

struct sqlite3;
struct sqlite3_api_routines;

#ifdef __cplusplus
namespace sqlite_protobuf {
extern "C" {
#endif

int sqlite3_sqliteprotobuf_init(struct sqlite3 *db, char **pzErrMsg,
    const struct sqlite3_api_routines *pApi);

#ifdef __cplusplus
}
}  // namespace sqlite_protobuf
#endif
