// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

/* A C interface to libnosql, for languages that call C (the C# binding in bindings/csharp uses it).
 *
 * Every function that can fail returns a status: >= 0 is success, < 0 is -(nosql::ErrorCode). The text of the
 * newest failure on the calling thread is nosql_last_error(). Lookups return 0 when the key was found and 1 when
 * it was not. Handles are opaque; free each with its _close/_free function. Keys and values are (pointer, length).
 *
 * Memory a read returns points into the store and is valid until the transaction ends (and, for a cursor, until it
 * moves). Nothing is copied. A handle may be used from one thread at a time. A transaction handle must not outlive
 * its environment handle. */
#pragma once

#include <stddef.h>
#include <stdint.h>

#if defined(_WIN32)
#define NOSQL_C_API __declspec(dllexport)
#else
#define NOSQL_C_API __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct nosql_env nosql_env;
typedef struct nosql_txn nosql_txn;
typedef struct nosql_db nosql_db;
typedef struct nosql_cursor nosql_cursor;

enum { NOSQL_DB_CREATE = 1, NOSQL_DB_INTEGER_KEY = 2, NOSQL_DB_REVERSE_KEY = 4, NOSQL_DB_PREFIX_KEYS = 8 };
enum { NOSQL_PUT_UPSERT = 0, NOSQL_PUT_INSERT_UNIQUE = 1, NOSQL_PUT_UPDATE_ONLY = 2, NOSQL_PUT_APPEND = 3 };
enum { NOSQL_SYNC_SAFE = 0, NOSQL_SYNC_NO_META = 1, NOSQL_SYNC_NONE = 2, NOSQL_SYNC_LAZY = 3 };

/* Zero the struct and set what you need; a zero field means the library's default. */
typedef struct nosql_options
{
    uint64_t page_size;
    uint64_t max_size;
    uint64_t initial_size;
    uint32_t max_dbs;
    int32_t durability;        /* NOSQL_SYNC_* */
    uint32_t sync_interval_ms; /* Lazy */
    uint8_t read_only;
    uint8_t error_if_exists;
    uint8_t no_create;
    uint8_t pipeline_commits;
    uint8_t background_sync;
    uint8_t allow_network_filesystem;
    uint8_t reserved[2];
    uint64_t buffer_cache;
} nosql_options;

typedef struct nosql_env_stats
{
    uint32_t page_size;
    uint32_t readers;
    uint64_t file_size, map_size, used_pages, free_pages, last_txn, oldest_reader, durable_txn;
} nosql_env_stats;

typedef struct nosql_tree_stats
{
    uint32_t page_size, depth;
    uint64_t branch_pages, leaf_pages, overflow_pages, entries;
} nosql_tree_stats;

NOSQL_C_API const char* nosql_version(void);
NOSQL_C_API const char* nosql_last_error(void);

NOSQL_C_API int nosql_env_open(const char* path, const nosql_options* options, nosql_env** out);
NOSQL_C_API void nosql_env_close(nosql_env* env);
NOSQL_C_API int nosql_env_sync(nosql_env* env, int force);
NOSQL_C_API int nosql_env_stats_get(nosql_env* env, nosql_env_stats* out);
NOSQL_C_API int nosql_env_check(nosql_env* env);
NOSQL_C_API uint64_t nosql_env_commit_generation(nosql_env* env);
NOSQL_C_API int nosql_compact(const char* src, const char* dst);

/* timeout_ms < 0 waits forever. */
NOSQL_C_API int nosql_txn_begin_write(nosql_env* env, int64_t timeout_ms, nosql_txn** out);
NOSQL_C_API int nosql_txn_begin_read(nosql_env* env, nosql_txn** out);
NOSQL_C_API int nosql_txn_nested(nosql_txn* parent, nosql_txn** out);
NOSQL_C_API int nosql_txn_commit(nosql_txn* txn); /* consumes the handle, success or not */
NOSQL_C_API void nosql_txn_abort(nosql_txn* txn); /* consumes the handle */
NOSQL_C_API uint64_t nosql_txn_id(nosql_txn* txn);
NOSQL_C_API int nosql_txn_read_only(nosql_txn* txn);
NOSQL_C_API int nosql_txn_has_db(nosql_txn* txn, const char* name, size_t name_len);
NOSQL_C_API int nosql_txn_drop_db(nosql_txn* txn, const char* name, size_t name_len);
/* The names of the sub-databases, NUL-separated, in a buffer the library owns (valid until the next call here). */
NOSQL_C_API int nosql_txn_list_dbs(nosql_txn* txn, const char** names, size_t* len, size_t* count);

/* name == NULL opens the main database. Free the handle with nosql_db_free; it stays valid until its transaction ends. */
NOSQL_C_API int nosql_db_open(nosql_txn* txn, const char* name, size_t name_len, uint32_t flags, nosql_db** out);
NOSQL_C_API void nosql_db_free(nosql_db* db);
NOSQL_C_API int nosql_db_get(nosql_db* db, const void* key, size_t key_len, const void** value, size_t* value_len);
NOSQL_C_API int nosql_db_put(nosql_db* db, const void* key, size_t key_len, const void* value, size_t value_len, int mode);
/* Reserves value_len bytes for the key and returns where to write them; fill them before the next call on the transaction. */
NOSQL_C_API int nosql_db_reserve(nosql_db* db, const void* key, size_t key_len, size_t value_len, int mode, void** out);
NOSQL_C_API int nosql_db_erase(nosql_db* db, const void* key, size_t key_len); /* 0 erased, 1 absent */
NOSQL_C_API int nosql_db_clear(nosql_db* db);
NOSQL_C_API int nosql_db_drop(nosql_db* db);
NOSQL_C_API int nosql_db_count(nosql_db* db, uint64_t* out);
NOSQL_C_API int nosql_db_count_range(nosql_db* db, const void* lo, size_t lo_len, const void* hi, size_t hi_len, uint64_t* out);
NOSQL_C_API int nosql_db_stats(nosql_db* db, nosql_tree_stats* out);

NOSQL_C_API int nosql_cursor_open(nosql_db* db, nosql_cursor** out);
NOSQL_C_API void nosql_cursor_free(nosql_cursor* cursor);
/* Each move returns 0 and the entry it landed on, or 1 when there is none. */
NOSQL_C_API int nosql_cursor_first(nosql_cursor* c, const void** key, size_t* key_len, const void** value, size_t* value_len);
NOSQL_C_API int nosql_cursor_last(nosql_cursor* c, const void** key, size_t* key_len, const void** value, size_t* value_len);
NOSQL_C_API int nosql_cursor_next(nosql_cursor* c, const void** key, size_t* key_len, const void** value, size_t* value_len);
NOSQL_C_API int nosql_cursor_prev(nosql_cursor* c, const void** key, size_t* key_len, const void** value, size_t* value_len);
/* The first entry whose key is >= the given one. */
NOSQL_C_API int nosql_cursor_seek(nosql_cursor* c, const void* key, size_t len, const void** k, size_t* k_len, const void** v, size_t* v_len);
NOSQL_C_API int nosql_cursor_seek_exact(nosql_cursor* c, const void* key, size_t len, const void** v, size_t* v_len);
/* The entry the cursor is on, 0, or 1 when it is on none. */
NOSQL_C_API int nosql_cursor_current(nosql_cursor* c, const void** k, size_t* k_len, const void** v, size_t* v_len);
NOSQL_C_API int nosql_cursor_put(nosql_cursor* c, const void* key, size_t key_len, const void* value, size_t value_len, int mode);
NOSQL_C_API int nosql_cursor_erase(nosql_cursor* c);
NOSQL_C_API int nosql_cursor_skip(nosql_cursor* c, uint64_t n); /* 0 moved, 1 ran off the end */

#ifdef __cplusplus
}
#endif
