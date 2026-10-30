// Copyright (c) 2026 Philipp Orlov

using System.Text;

namespace Nosql;

public enum ErrorCode
{
    Ok = 0, NotFound, KeyExists, Corrupted, InvalidArgument, Unsupported, KeyTooLarge, ValueTooLarge, MapFull, TooManyDbs,
    Incompatible, BadTransaction, ReadOnly, Busy, IoError, OutOfMemory, Conflict,
}

public sealed class NosqlException : Exception
{
    public ErrorCode Code { get; }
    public NosqlException(ErrorCode code, string message) : base(message) => Code = code;
}

[Flags]
public enum DbFlags : uint { None = 0, Create = 1, IntegerKey = 2, ReverseKey = 4, PrefixKeys = 8 }

public enum PutMode { Upsert = 0, InsertUnique = 1, UpdateOnly = 2, Append = 3 }

/// <summary>How much the store flushes on commit; see the C++ documentation of Durability.</summary>
public enum Durability { Safe = 0, NoMetaSync = 1, None = 2, Lazy = 3 }

/// <summary>Zero in a numeric field means the library's default.</summary>
public sealed class EnvOptions
{
    public ulong PageSize, MaxSize, InitialSize, BufferCache;
    public uint MaxDbs;
    public Durability Sync = Durability.Safe;
    public TimeSpan SyncInterval;
    public bool ReadOnly, ErrorIfExists, CreateIfMissing = true, PipelineCommits, BackgroundSync, AllowNetworkFilesystem;
}

public readonly record struct EnvStats(uint PageSize, uint Readers, ulong FileSize, ulong MapSize, ulong UsedPages, ulong FreePages,
                                       ulong LastTxn, ulong OldestReader, ulong DurableTxn);
public readonly record struct TreeStats(uint PageSize, uint Depth, ulong BranchPages, ulong LeafPages, ulong OverflowPages, ulong Entries);

/// <summary>A store: one file, one writer at a time, any number of readers on snapshots.</summary>
public sealed class Env : IDisposable
{
    IntPtr _h;

    Env(IntPtr h) => _h = h;

    public static string Version => System.Runtime.InteropServices.Marshal.PtrToStringUTF8(Native.nosql_version()) ?? "";

    public static Env Open(string path, EnvOptions? options = null)
    {
        options ??= new EnvOptions();
        var o = new Native.Options
        {
            PageSize = options.PageSize, MaxSize = options.MaxSize, InitialSize = options.InitialSize, MaxDbs = options.MaxDbs,
            Durability = (int)options.Sync, SyncIntervalMs = (uint)options.SyncInterval.TotalMilliseconds,
            ReadOnly = (byte)(options.ReadOnly ? 1 : 0), ErrorIfExists = (byte)(options.ErrorIfExists ? 1 : 0),
            NoCreate = (byte)(options.CreateIfMissing ? 0 : 1), PipelineCommits = (byte)(options.PipelineCommits ? 1 : 0),
            BackgroundSync = (byte)(options.BackgroundSync ? 1 : 0), AllowNetworkFilesystem = (byte)(options.AllowNetworkFilesystem ? 1 : 0),
            BufferCache = options.BufferCache,
        };
        Native.Check(Native.nosql_env_open(path, o, out var h));
        return new Env(h);
    }

    IntPtr Handle => _h != IntPtr.Zero ? _h : throw new ObjectDisposedException(nameof(Env));

    /// <summary>Starts the (single) write transaction, waiting for the current one to end. Dispose it to abort, or Commit it.</summary>
    public Txn WriteTxn(TimeSpan? timeout = null)
    {
        Native.Check(Native.nosql_txn_begin_write(Handle, timeout is { } t ? (long)t.TotalMilliseconds : -1, out var h));
        return new Txn(h);
    }

    /// <summary>A snapshot of the store as of now; later commits do not show in it.</summary>
    public Txn ReadTxn()
    {
        Native.Check(Native.nosql_txn_begin_read(Handle, out var h));
        return new Txn(h);
    }

    /// <summary>Runs the body in a write transaction and commits it unless it throws.</summary>
    public T Write<T>(Func<Txn, T> body)
    {
        using var t = WriteTxn();
        var r = body(t);
        t.Commit();
        return r;
    }

    public void Write(Action<Txn> body) => Write<object?>(t => { body(t); return null; });

    public T Read<T>(Func<Txn, T> body)
    {
        using var t = ReadTxn();
        return body(t);
    }

    public void Sync(bool force = true) => Native.Check(Native.nosql_env_sync(Handle, force ? 1 : 0));

    /// <summary>Walks the whole store and throws when a structure is inconsistent.</summary>
    public void CheckIntegrity() => Native.Check(Native.nosql_env_check(Handle));

    public ulong CommitGeneration => Native.nosql_env_commit_generation(Handle);

    public EnvStats Stats()
    {
        Native.Check(Native.nosql_env_stats_get(Handle, out var s));
        return new EnvStats(s.PageSize, s.Readers, s.FileSize, s.MapSize, s.UsedPages, s.FreePages, s.LastTxn, s.OldestReader, s.DurableTxn);
    }

    /// <summary>Copies the live data of a store into a new file without the free space.</summary>
    public static void Compact(string source, string destination) => Native.Check(Native.nosql_compact(source, destination));

    public void Dispose()
    {
        var h = _h;
        _h = IntPtr.Zero;
        if (h != IntPtr.Zero) Native.nosql_env_close(h);
    }
}

/// <summary>A transaction. Everything read inside it, spans included, is valid until it ends.</summary>
public sealed unsafe class Txn : IDisposable
{
    IntPtr _h;

    internal Txn(IntPtr h) => _h = h;
    internal IntPtr Handle => _h != IntPtr.Zero ? _h : throw new ObjectDisposedException(nameof(Txn));

    public bool IsReadOnly => Native.nosql_txn_read_only(Handle) != 0;
    public ulong Id => Native.nosql_txn_id(Handle);

    /// <summary>A sub-database; pass <c>null</c> for the main one. Writers create what is missing with <see cref="DbFlags.Create"/>.</summary>
    public Database Db(string? name = null, DbFlags flags = DbFlags.None)
    {
        if (name == null)
        {
            Native.Check(Native.nosql_db_open(Handle, null, 0, (uint)flags, out var main));
            return new Database(main);
        }
        var bytes = Encoding.UTF8.GetBytes(name);
        fixed (byte* p = bytes)
        {
            Native.Check(Native.nosql_db_open(Handle, p, (nuint)bytes.Length, (uint)flags, out var h));
            return new Database(h);
        }
    }

    public bool HasDb(string name)
    {
        var bytes = Encoding.UTF8.GetBytes(name);
        fixed (byte* p = bytes) return Native.Check(Native.nosql_txn_has_db(Handle, p, (nuint)bytes.Length)) != 0;
    }

    public void DropDb(string name)
    {
        var bytes = Encoding.UTF8.GetBytes(name);
        fixed (byte* p = bytes) Native.Check(Native.nosql_txn_drop_db(Handle, p, (nuint)bytes.Length));
    }

    public IReadOnlyList<string> ListDbs()
    {
        Native.Check(Native.nosql_txn_list_dbs(Handle, out var names, out var len, out var count));
        var all = Encoding.UTF8.GetString(new ReadOnlySpan<byte>(names, (int)len));
        return all.Length == 0 ? Array.Empty<string>() : all.TrimEnd('\0').Split('\0');
    }

    /// <summary>A transaction inside this one; its commit joins the parent's, its abort leaves the parent untouched.</summary>
    public Txn Nested()
    {
        Native.Check(Native.nosql_txn_nested(Handle, out var h));
        return new Txn(h);
    }

    public void Commit()
    {
        var h = Handle;
        _h = IntPtr.Zero;
        Native.Check(Native.nosql_txn_commit(h));
    }

    public void Abort() => Dispose();

    public void Dispose()
    {
        var h = _h;
        _h = IntPtr.Zero;
        if (h != IntPtr.Zero) Native.nosql_txn_abort(h);
    }
}

/// <summary>A sorted map of byte keys to byte values inside a transaction.</summary>
public sealed unsafe class Database : IDisposable
{
    IntPtr _h;

    internal Database(IntPtr h) => _h = h;
    internal IntPtr Handle => _h != IntPtr.Zero ? _h : throw new ObjectDisposedException(nameof(Database));

    /// <summary>The value of the key, looked at in place (no copy); false when the key is absent.</summary>
    public bool TryGet(ReadOnlySpan<byte> key, out ReadOnlySpan<byte> value)
    {
        fixed (byte* k = key)
        {
            int rc = Native.Check(Native.nosql_db_get(Handle, k, (nuint)key.Length, out var v, out var n));
            value = rc == 0 ? new ReadOnlySpan<byte>(v, (int)n) : default;
            return rc == 0;
        }
    }

    public byte[]? Get(ReadOnlySpan<byte> key) => TryGet(key, out var v) ? v.ToArray() : null;
    public string? GetString(string key) => TryGet(Encoding.UTF8.GetBytes(key), out var v) ? Encoding.UTF8.GetString(v) : null;
    public bool Contains(ReadOnlySpan<byte> key) => TryGet(key, out _);

    /// <summary>Stores the pair; false only when the mode refused (InsertUnique found it, UpdateOnly did not).</summary>
    public bool Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, PutMode mode = PutMode.Upsert)
    {
        fixed (byte* k = key)
        fixed (byte* v = value)
            return Native.Check(Native.nosql_db_put(Handle, k, (nuint)key.Length, v, (nuint)value.Length, (int)mode)) == 0;
    }

    public bool Put(string key, string value, PutMode mode = PutMode.Upsert) => Put(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), mode);

    /// <summary>Makes room for a value of the given size and returns it to be filled in place, until the next call on the transaction.</summary>
    public Span<byte> Reserve(ReadOnlySpan<byte> key, int size, PutMode mode = PutMode.Upsert)
    {
        fixed (byte* k = key)
        {
            Native.Check(Native.nosql_db_reserve(Handle, k, (nuint)key.Length, (nuint)size, (int)mode, out var dest));
            return new Span<byte>(dest, size);
        }
    }

    public bool Erase(ReadOnlySpan<byte> key)
    {
        fixed (byte* k = key) return Native.Check(Native.nosql_db_erase(Handle, k, (nuint)key.Length)) == 0;
    }

    public bool Erase(string key) => Erase(Encoding.UTF8.GetBytes(key));

    public void Clear() => Native.Check(Native.nosql_db_clear(Handle));
    public void Drop() => Native.Check(Native.nosql_db_drop(Handle));

    public ulong Count
    {
        get { Native.Check(Native.nosql_db_count(Handle, out var n)); return n; }
    }

    /// <summary>Entries with lo &lt;= key &lt; hi.</summary>
    public ulong CountRange(ReadOnlySpan<byte> lo, ReadOnlySpan<byte> hi)
    {
        fixed (byte* l = lo)
        fixed (byte* h = hi)
        {
            Native.Check(Native.nosql_db_count_range(Handle, l, (nuint)lo.Length, h, (nuint)hi.Length, out var n));
            return n;
        }
    }

    public TreeStats Stats()
    {
        Native.Check(Native.nosql_db_stats(Handle, out var s));
        return new TreeStats(s.PageSize, s.Depth, s.BranchPages, s.LeafPages, s.OverflowPages, s.Entries);
    }

    public Cursor Cursor()
    {
        Native.Check(Native.nosql_cursor_open(Handle, out var h));
        return new Cursor(h);
    }

    /// <summary>Entries with the given key prefix, in order, as copies.</summary>
    public IEnumerable<KeyValuePair<byte[], byte[]>> Scan(ReadOnlyMemory<byte> prefix = default)
    {
        using var c = Cursor();
        for (bool ok = prefix.IsEmpty ? c.First() : c.Seek(prefix.Span); ok && c.Key.StartsWith(prefix.Span); ok = c.Next())
            yield return new(c.Key.ToArray(), c.Value.ToArray());
    }

    public void Dispose()
    {
        var h = _h;
        _h = IntPtr.Zero;
        if (h != IntPtr.Zero) Native.nosql_db_free(h);
    }
}

/// <summary>A position in a database. Key and Value point into the store and stay valid until the cursor moves.</summary>
public sealed unsafe class Cursor : IDisposable
{
    IntPtr _h;
    byte* _k, _v;
    nuint _kl, _vl;
    bool _valid;

    internal Cursor(IntPtr h) => _h = h;
    IntPtr Handle => _h != IntPtr.Zero ? _h : throw new ObjectDisposedException(nameof(Cursor));

    public bool IsValid => _valid;
    public ReadOnlySpan<byte> Key => _valid ? new ReadOnlySpan<byte>(_k, (int)_kl) : throw new InvalidOperationException("the cursor is not on an entry");
    public ReadOnlySpan<byte> Value => _valid ? new ReadOnlySpan<byte>(_v, (int)_vl) : throw new InvalidOperationException("the cursor is not on an entry");

    bool Landed(int status) => _valid = Native.Check(status) == 0;

    public bool First() => Landed(Native.nosql_cursor_first(Handle, out _k, out _kl, out _v, out _vl));
    public bool Last() => Landed(Native.nosql_cursor_last(Handle, out _k, out _kl, out _v, out _vl));
    public bool Next() => Landed(Native.nosql_cursor_next(Handle, out _k, out _kl, out _v, out _vl));
    public bool Prev() => Landed(Native.nosql_cursor_prev(Handle, out _k, out _kl, out _v, out _vl));

    /// <summary>Moves to the first entry whose key is at or after the given one.</summary>
    public bool Seek(ReadOnlySpan<byte> key)
    {
        fixed (byte* p = key) return Landed(Native.nosql_cursor_seek(Handle, p, (nuint)key.Length, out _k, out _kl, out _v, out _vl));
    }

    /// <summary>Moves to the key; false (and no position) when it is absent.</summary>
    public bool SeekExact(ReadOnlySpan<byte> key)
    {
        fixed (byte* p = key)
        {
            if (Native.Check(Native.nosql_cursor_seek_exact(Handle, p, (nuint)key.Length, out _, out _)) != 0) return _valid = false;
        }
        return Landed(Native.nosql_cursor_current(Handle, out _k, out _kl, out _v, out _vl));
    }

    bool Refresh() => Landed(Native.nosql_cursor_current(Handle, out _k, out _kl, out _v, out _vl));

    /// <summary>Moves n entries forward; false (and no position) past the last.</summary>
    public bool Skip(ulong n) => Native.Check(Native.nosql_cursor_skip(Handle, n)) == 0 ? Refresh() : _valid = false;

    public bool Put(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value, PutMode mode = PutMode.Upsert)
    {
        fixed (byte* k = key)
        fixed (byte* v = value)
        {
            bool stored = Native.Check(Native.nosql_cursor_put(Handle, k, (nuint)key.Length, v, (nuint)value.Length, (int)mode)) == 0;
            if (stored) Refresh();
            return stored;
        }
    }

    /// <summary>Erases the entry under the cursor and moves to the next; false at the end.</summary>
    public bool Erase() => Native.Check(Native.nosql_cursor_erase(Handle)) == 0 ? Refresh() : _valid = false;

    public void Dispose()
    {
        var h = _h;
        _h = IntPtr.Zero;
        _valid = false;
        if (h != IntPtr.Zero) Native.nosql_cursor_free(h);
    }
}
