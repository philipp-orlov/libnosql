// Copyright (c) 2026 Philipp Orlov
// SPDX-License-Identifier: Apache-2.0

// The C interface (include/nosql/nosql_c.h): handles around the C++ objects, a status for every failure.
#include "nosql/nosql_c.h"

#include <chrono>
#include <exception>
#include <new>
#include <string>

#include "nosql/nosql.hpp"

struct nosql_env
{
    nosql::Env env;
};
struct nosql_txn
{
    nosql::Txn txn;
};
struct nosql_db
{
    nosql::Db db;
};
struct nosql_cursor
{
    nosql::Cursor cursor;
};

namespace {

thread_local std::string lastError;

int fail(nosql::ErrorCode code, const char* what)
{
    lastError = what;
    return -static_cast<int>(code);
}

// Runs fn, which returns a status, and turns what it throws into one.
template <class F>
int guarded(F&& fn) noexcept
{
    try {
        return fn();
    } catch (const nosql::Error& e) {
        return fail(e.code(), e.what());
    } catch (const std::bad_alloc&) {
        return fail(nosql::ErrorCode::OutOfMemory, "out of memory");
    } catch (const std::exception& e) {
        return fail(nosql::ErrorCode::InvalidArgument, e.what());
    } catch (...) {
        return fail(nosql::ErrorCode::InvalidArgument, "unknown error");
    }
}

nosql::Slice slice(const void* p, size_t n) noexcept { return nosql::Slice(p, n); }

void give(nosql::Slice s, const void** p, size_t* n) noexcept
{
    *p = s.data();
    *n = s.size();
}

int entry(nosql::Cursor& c, bool landed, const void** k, size_t* kl, const void** v, size_t* vl)
{
    if (!landed)
        return 1;
    give(c.key(), k, kl);
    give(c.value(), v, vl);
    return 0;
}

nosql::PutMode putMode(int mode) { return static_cast<nosql::PutMode>(mode); }

}  // namespace

extern "C" {

const char* nosql_version(void) { return nosql::version(); }
const char* nosql_last_error(void) { return lastError.c_str(); }

int nosql_env_open(const char* path, const nosql_options* o, nosql_env** out)
{
    return guarded([&] {
        auto opts = nosql::Env::configure();
        if (o) {
            if (o->page_size) opts.pageSize(o->page_size);
            if (o->max_size) opts.maxSize(o->max_size);
            if (o->initial_size) opts.initialSize(o->initial_size);
            if (o->max_dbs) opts.maxDbs(o->max_dbs);
            opts.sync(static_cast<nosql::Durability>(o->durability));
            if (o->sync_interval_ms) opts.syncInterval(std::chrono::milliseconds(o->sync_interval_ms));
            opts.readOnly(o->read_only != 0);
            opts.errorIfExists(o->error_if_exists != 0);
            opts.createIfMissing(o->no_create == 0);
            opts.pipelineCommits(o->pipeline_commits != 0);
            opts.backgroundSync(o->background_sync != 0);
            opts.allowNetworkFilesystem(o->allow_network_filesystem != 0);
            if (o->buffer_cache) opts.bufferCache(o->buffer_cache);
        }
        *out = new nosql_env{opts.open(path)};
        return 0;
    });
}

void nosql_env_close(nosql_env* env)
{
    if (!env)
        return;
    try {
        env->env.close();
    } catch (...) {
    }
    delete env;
}

int nosql_env_sync(nosql_env* env, int force)
{
    return guarded([&] {
        env->env.sync(force != 0);
        return 0;
    });
}

int nosql_env_stats_get(nosql_env* env, nosql_env_stats* out)
{
    return guarded([&] {
        auto s = env->env.stats();
        out->page_size = s.pageSize;
        out->readers = s.readers;
        out->file_size = s.fileSize;
        out->map_size = s.mapSize;
        out->used_pages = s.usedPages;
        out->free_pages = s.freePages;
        out->last_txn = s.lastTxn;
        out->oldest_reader = s.oldestReader;
        out->durable_txn = s.durableTxn;
        return 0;
    });
}

int nosql_env_check(nosql_env* env)
{
    return guarded([&] {
        env->env.read([](nosql::Txn& t) { nosql::checkIntegrity(t); });
        return 0;
    });
}

uint64_t nosql_env_commit_generation(nosql_env* env) { return env->env.commitGeneration(); }

int nosql_compact(const char* src, const char* dst)
{
    return guarded([&] {
        nosql::compact(src, dst);
        return 0;
    });
}

int nosql_txn_begin_write(nosql_env* env, int64_t timeout_ms, nosql_txn** out)
{
    return guarded([&] {
        *out = new nosql_txn{timeout_ms < 0 ? env->env.writeTxn() : env->env.writeTxn(std::chrono::milliseconds(timeout_ms))};
        return 0;
    });
}

int nosql_txn_begin_read(nosql_env* env, nosql_txn** out)
{
    return guarded([&] {
        *out = new nosql_txn{env->env.readTxn()};
        return 0;
    });
}

int nosql_txn_nested(nosql_txn* parent, nosql_txn** out)
{
    return guarded([&] {
        *out = new nosql_txn{parent->txn.nested()};
        return 0;
    });
}

int nosql_txn_commit(nosql_txn* txn)
{
    int rc = guarded([&] {
        txn->txn.commit();
        return 0;
    });
    delete txn;
    return rc;
}

void nosql_txn_abort(nosql_txn* txn)
{
    if (!txn)
        return;
    try {
        txn->txn.abort();
    } catch (...) {
    }
    delete txn;
}

uint64_t nosql_txn_id(nosql_txn* txn) { return txn->txn.id(); }
int nosql_txn_read_only(nosql_txn* txn) { return txn->txn.isReadOnly() ? 1 : 0; }

int nosql_txn_has_db(nosql_txn* txn, const char* name, size_t n)
{
    return guarded([&] { return txn->txn.hasDb(slice(name, n)) ? 1 : 0; });
}

int nosql_txn_drop_db(nosql_txn* txn, const char* name, size_t n)
{
    return guarded([&] {
        txn->txn.dropDb(slice(name, n));
        return 0;
    });
}

int nosql_txn_list_dbs(nosql_txn* txn, const char** names, size_t* len, size_t* count)
{
    thread_local std::string joined;
    return guarded([&] {
        joined.clear();
        auto list = txn->txn.listDbs();
        for (const std::string& s : list) {
            joined += s;
            joined += '\0';
        }
        *names = joined.data();
        *len = joined.size();
        *count = list.size();
        return 0;
    });
}

int nosql_db_open(nosql_txn* txn, const char* name, size_t n, uint32_t flags, nosql_db** out)
{
    return guarded([&] {
        *out = new nosql_db{name ? txn->txn.db(slice(name, n), static_cast<nosql::DbFlags>(flags)) : txn->txn.mainDb()};
        return 0;
    });
}

void nosql_db_free(nosql_db* db) { delete db; }

int nosql_db_get(nosql_db* db, const void* key, size_t kl, const void** value, size_t* vl)
{
    return guarded([&] {
        auto v = db->db.get(slice(key, kl));
        if (!v)
            return 1;
        give(*v, value, vl);
        return 0;
    });
}

int nosql_db_put(nosql_db* db, const void* key, size_t kl, const void* value, size_t vl, int mode)
{
    return guarded([&] { return db->db.put(slice(key, kl), slice(value, vl), putMode(mode)) ? 0 : 1; });
}

int nosql_db_reserve(nosql_db* db, const void* key, size_t kl, size_t vl, int mode, void** out)
{
    return guarded([&] {
        *out = db->db.reserve(slice(key, kl), vl, putMode(mode)).data();
        return 0;
    });
}

int nosql_db_erase(nosql_db* db, const void* key, size_t kl)
{
    return guarded([&] { return db->db.erase(slice(key, kl)) ? 0 : 1; });
}

int nosql_db_clear(nosql_db* db)
{
    return guarded([&] {
        db->db.clear();
        return 0;
    });
}

int nosql_db_drop(nosql_db* db)
{
    return guarded([&] {
        db->db.drop();
        return 0;
    });
}

int nosql_db_count(nosql_db* db, uint64_t* out)
{
    return guarded([&] {
        *out = db->db.count();
        return 0;
    });
}

int nosql_db_count_range(nosql_db* db, const void* lo, size_t ll, const void* hi, size_t hl, uint64_t* out)
{
    return guarded([&] {
        *out = db->db.countRange(slice(lo, ll), slice(hi, hl));
        return 0;
    });
}

int nosql_db_stats(nosql_db* db, nosql_tree_stats* out)
{
    return guarded([&] {
        auto s = db->db.stats();
        out->page_size = s.pageSize;
        out->depth = s.depth;
        out->branch_pages = s.branchPages;
        out->leaf_pages = s.leafPages;
        out->overflow_pages = s.overflowPages;
        out->entries = s.entries;
        return 0;
    });
}

int nosql_cursor_open(nosql_db* db, nosql_cursor** out)
{
    return guarded([&] {
        *out = new nosql_cursor{db->db.cursor()};
        return 0;
    });
}

void nosql_cursor_free(nosql_cursor* c) { delete c; }

int nosql_cursor_first(nosql_cursor* c, const void** k, size_t* kl, const void** v, size_t* vl)
{
    return guarded([&] { return entry(c->cursor, c->cursor.first(), k, kl, v, vl); });
}
int nosql_cursor_last(nosql_cursor* c, const void** k, size_t* kl, const void** v, size_t* vl)
{
    return guarded([&] { return entry(c->cursor, c->cursor.last(), k, kl, v, vl); });
}
int nosql_cursor_next(nosql_cursor* c, const void** k, size_t* kl, const void** v, size_t* vl)
{
    return guarded([&] { return entry(c->cursor, c->cursor.next(), k, kl, v, vl); });
}
int nosql_cursor_prev(nosql_cursor* c, const void** k, size_t* kl, const void** v, size_t* vl)
{
    return guarded([&] { return entry(c->cursor, c->cursor.prev(), k, kl, v, vl); });
}
int nosql_cursor_seek(nosql_cursor* c, const void* key, size_t len, const void** k, size_t* kl, const void** v, size_t* vl)
{
    return guarded([&] { return entry(c->cursor, c->cursor.seek(slice(key, len)), k, kl, v, vl); });
}

int nosql_cursor_seek_exact(nosql_cursor* c, const void* key, size_t len, const void** v, size_t* vl)
{
    return guarded([&] {
        if (!c->cursor.seekExact(slice(key, len)))
            return 1;
        give(c->cursor.value(), v, vl);
        return 0;
    });
}

int nosql_cursor_current(nosql_cursor* c, const void** k, size_t* kl, const void** v, size_t* vl)
{
    return guarded([&] { return entry(c->cursor, c->cursor.valid(), k, kl, v, vl); });
}

int nosql_cursor_put(nosql_cursor* c, const void* key, size_t kl, const void* value, size_t vl, int mode)
{
    return guarded([&] { return c->cursor.put(slice(key, kl), slice(value, vl), putMode(mode)) ? 0 : 1; });
}

int nosql_cursor_erase(nosql_cursor* c)
{
    return guarded([&] { return c->cursor.erase() ? 0 : 1; });
}

int nosql_cursor_skip(nosql_cursor* c, uint64_t n)
{
    return guarded([&] { return c->cursor.skip(n) ? 0 : 1; });
}

}  // extern "C"
