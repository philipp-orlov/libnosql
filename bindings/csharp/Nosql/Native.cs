// Copyright (c) 2026 Philipp Orlov

using System.Reflection;
using System.Runtime.InteropServices;

namespace Nosql;

/// <summary>The C interface (include/nosql/nosql_c.h). Status: &gt;= 0 success, &lt; 0 is -(ErrorCode).</summary>
internal static unsafe partial class Native
{
    const string Lib = "nosql_c";

    static Native()
    {
        // NOSQL_LIBRARY names the shared library to load when it is not next to the assembly or on the loader path.
        NativeLibrary.SetDllImportResolver(typeof(Native).Assembly, (name, assembly, path) =>
        {
            if (name != Lib) return IntPtr.Zero;
            var explicitPath = Environment.GetEnvironmentVariable("NOSQL_LIBRARY");
            if (!string.IsNullOrEmpty(explicitPath) && NativeLibrary.TryLoad(explicitPath, out var h)) return h;
            return NativeLibrary.TryLoad(name, assembly, path, out h) ? h : IntPtr.Zero;
        });
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct Options
    {
        public ulong PageSize, MaxSize, InitialSize;
        public uint MaxDbs;
        public int Durability;
        public uint SyncIntervalMs;
        public byte ReadOnly, ErrorIfExists, NoCreate, PipelineCommits, BackgroundSync, AllowNetworkFilesystem, Reserved0, Reserved1;
        public ulong BufferCache;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct EnvStats
    {
        public uint PageSize, Readers;
        public ulong FileSize, MapSize, UsedPages, FreePages, LastTxn, OldestReader, DurableTxn;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct TreeStats
    {
        public uint PageSize, Depth;
        public ulong BranchPages, LeafPages, OverflowPages, Entries;
    }

    [LibraryImport(Lib)] public static partial IntPtr nosql_version();
    [LibraryImport(Lib)] public static partial IntPtr nosql_last_error();

    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)] public static partial int nosql_env_open(string path, in Options options, out IntPtr env);
    [LibraryImport(Lib)] public static partial void nosql_env_close(IntPtr env);
    [LibraryImport(Lib)] public static partial int nosql_env_sync(IntPtr env, int force);
    [LibraryImport(Lib)] public static partial int nosql_env_stats_get(IntPtr env, out EnvStats stats);
    [LibraryImport(Lib)] public static partial int nosql_env_check(IntPtr env);
    [LibraryImport(Lib)] public static partial ulong nosql_env_commit_generation(IntPtr env);
    [LibraryImport(Lib, StringMarshalling = StringMarshalling.Utf8)] public static partial int nosql_compact(string src, string dst);

    [LibraryImport(Lib)] public static partial int nosql_txn_begin_write(IntPtr env, long timeoutMs, out IntPtr txn);
    [LibraryImport(Lib)] public static partial int nosql_txn_begin_read(IntPtr env, out IntPtr txn);
    [LibraryImport(Lib)] public static partial int nosql_txn_nested(IntPtr parent, out IntPtr txn);
    [LibraryImport(Lib)] public static partial int nosql_txn_commit(IntPtr txn);
    [LibraryImport(Lib)] public static partial void nosql_txn_abort(IntPtr txn);
    [LibraryImport(Lib)] public static partial ulong nosql_txn_id(IntPtr txn);
    [LibraryImport(Lib)] public static partial int nosql_txn_read_only(IntPtr txn);
    [LibraryImport(Lib)] public static partial int nosql_txn_has_db(IntPtr txn, byte* name, nuint len);
    [LibraryImport(Lib)] public static partial int nosql_txn_drop_db(IntPtr txn, byte* name, nuint len);
    [LibraryImport(Lib)] public static partial int nosql_txn_list_dbs(IntPtr txn, out byte* names, out nuint len, out nuint count);

    [LibraryImport(Lib)] public static partial int nosql_db_open(IntPtr txn, byte* name, nuint len, uint flags, out IntPtr db);
    [LibraryImport(Lib)] public static partial void nosql_db_free(IntPtr db);
    [LibraryImport(Lib)] public static partial int nosql_db_get(IntPtr db, byte* key, nuint keyLen, out byte* value, out nuint valueLen);
    [LibraryImport(Lib)] public static partial int nosql_db_put(IntPtr db, byte* key, nuint keyLen, byte* value, nuint valueLen, int mode);
    [LibraryImport(Lib)] public static partial int nosql_db_reserve(IntPtr db, byte* key, nuint keyLen, nuint valueLen, int mode, out byte* dest);
    [LibraryImport(Lib)] public static partial int nosql_db_erase(IntPtr db, byte* key, nuint keyLen);
    [LibraryImport(Lib)] public static partial int nosql_db_clear(IntPtr db);
    [LibraryImport(Lib)] public static partial int nosql_db_drop(IntPtr db);
    [LibraryImport(Lib)] public static partial int nosql_db_count(IntPtr db, out ulong count);
    [LibraryImport(Lib)] public static partial int nosql_db_count_range(IntPtr db, byte* lo, nuint loLen, byte* hi, nuint hiLen, out ulong count);
    [LibraryImport(Lib)] public static partial int nosql_db_stats(IntPtr db, out TreeStats stats);

    [LibraryImport(Lib)] public static partial int nosql_cursor_open(IntPtr db, out IntPtr cursor);
    [LibraryImport(Lib)] public static partial void nosql_cursor_free(IntPtr cursor);
    [LibraryImport(Lib)] public static partial int nosql_cursor_first(IntPtr c, out byte* k, out nuint kl, out byte* v, out nuint vl);
    [LibraryImport(Lib)] public static partial int nosql_cursor_last(IntPtr c, out byte* k, out nuint kl, out byte* v, out nuint vl);
    [LibraryImport(Lib)] public static partial int nosql_cursor_next(IntPtr c, out byte* k, out nuint kl, out byte* v, out nuint vl);
    [LibraryImport(Lib)] public static partial int nosql_cursor_prev(IntPtr c, out byte* k, out nuint kl, out byte* v, out nuint vl);
    [LibraryImport(Lib)] public static partial int nosql_cursor_seek(IntPtr c, byte* key, nuint len, out byte* k, out nuint kl, out byte* v, out nuint vl);
    [LibraryImport(Lib)] public static partial int nosql_cursor_seek_exact(IntPtr c, byte* key, nuint len, out byte* v, out nuint vl);
    [LibraryImport(Lib)] public static partial int nosql_cursor_current(IntPtr c, out byte* k, out nuint kl, out byte* v, out nuint vl);
    [LibraryImport(Lib)] public static partial int nosql_cursor_put(IntPtr c, byte* key, nuint kl, byte* value, nuint vl, int mode);
    [LibraryImport(Lib)] public static partial int nosql_cursor_erase(IntPtr c);
    [LibraryImport(Lib)] public static partial int nosql_cursor_skip(IntPtr c, ulong n);

    public static string LastError() => Marshal.PtrToStringUTF8(nosql_last_error()) ?? "";

    /// <summary>Throws for a negative status and returns the status otherwise.</summary>
    public static int Check(int status)
    {
        if (status < 0) throw new NosqlException((ErrorCode)(-status), LastError());
        return status;
    }
}
