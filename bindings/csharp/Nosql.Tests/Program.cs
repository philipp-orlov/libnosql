// Copyright (c) 2026 Philipp Orlov

// Exercises the C# binding of libnosql.   dotnet run -- <work dir>      (NOSQL_LIBRARY names libnosql_c)
using System.Text;
using Nosql;

int failures = 0, checks = 0;
void Check(bool ok, string what)
{
    checks++;
    if (!ok) { failures++; Console.Error.WriteLine($"  CHECK FAILED: {what}"); }
}
void Test(string name, Action body)
{
    try { body(); Console.WriteLine($"ok - {name}"); }
    catch (Exception e) { failures++; Console.Error.WriteLine($"not ok - {name}: {e}"); }
}
void Throws(ErrorCode code, Action body, string what)
{
    try { body(); Check(false, $"{what}: nothing thrown"); }
    catch (NosqlException e) { Check(e.Code == code, $"{what}: expected {code}, got {e.Code} ({e.Message})"); }
}

var work = args.Length > 0 ? args[0] : Path.Combine(Path.GetTempPath(), "nosql-cs-" + Environment.ProcessId);
Directory.CreateDirectory(work);
var path = Path.Combine(work, "test.nosql");
foreach (var f in Directory.GetFiles(work)) File.Delete(f);
static byte[] B(string s) => Encoding.UTF8.GetBytes(s);

Test("version", () => Check(Env.Version.Length > 0, "version"));

Test("put, get, erase and commit", () =>
{
    using var env = Env.Open(path);
    using (var t = env.WriteTxn())
    {
        using var db = t.Db();
        Check(db.Put("alpha", "1"), "put");
        Check(db.Put("beta", "2"), "put");
        Check(db.GetString("alpha") == "1", "read own write");
        Check(!db.Put(B("alpha"), B("x"), PutMode.InsertUnique), "insert unique refuses");
        Check(!db.Put(B("nope"), B("x"), PutMode.UpdateOnly), "update only refuses");
        Check(db.Count == 2, "count");
        t.Commit();
    }
    using var r = env.ReadTxn();
    using var d = r.Db();
    Check(r.IsReadOnly && d.GetString("beta") == "2" && d.GetString("gamma") == null, "committed data");
    Check(d.TryGet(B("alpha"), out var v) && v.SequenceEqual(B("1")), "zero-copy view");
});

Test("abort leaves nothing and dispose aborts", () =>
{
    using var env = Env.Open(path);
    using (var t = env.WriteTxn()) { using var db = t.Db(); db.Put("temp", "x"); }
    Check(env.Read(t => { using var db = t.Db(); return db.GetString("temp"); }) == null, "aborted write is gone");
});

Test("snapshot isolation", () =>
{
    using var env = Env.Open(path);
    using var snap = env.ReadTxn();
    env.Write(t => { using var db = t.Db(); db.Put("alpha", "changed"); });
    using var old = snap.Db();
    Check(old.GetString("alpha") == "1", "reader keeps its snapshot");
    Check(env.Read(t => { using var db = t.Db(); return db.GetString("alpha"); }) == "changed", "new reader sees the commit");
});

Test("nested transactions", () =>
{
    using var env = Env.Open(path);
    using var t = env.WriteTxn();
    using var db = t.Db();
    using (var n = t.Nested()) { using var d = n.Db(); d.Put("kept", "1"); n.Commit(); }
    using (var n = t.Nested()) { using var d = n.Db(); d.Put("dropped", "1"); }
    Check(db.GetString("kept") == "1" && db.GetString("dropped") == null, "nested commit joins, abort does not");
});

Test("sub-databases", () =>
{
    using var env = Env.Open(path);
    env.Write(t =>
    {
        using var users = t.Db("users", DbFlags.Create);
        using var ids = t.Db("ids", DbFlags.Create | DbFlags.IntegerKey);
        users.Put("ann", "x");
        ids.Put(BitConverter.GetBytes(7u), B("seven"));
        ids.Put(BitConverter.GetBytes(300u), B("three hundred"));
        ids.Put(BitConverter.GetBytes(2u), B("two"));
    });
    env.Read(t =>
    {
        Check(t.HasDb("users") && !t.HasDb("ghosts"), "has");
        Check(t.ListDbs().OrderBy(x => x).SequenceEqual(new[] { "ids", "users" }), "list");
        using var ids = t.Db("ids", DbFlags.IntegerKey);
        using var c = ids.Cursor();
        var order = new List<uint>();
        for (bool ok = c.First(); ok; ok = c.Next()) order.Add(BitConverter.ToUInt32(c.Key));
        Check(order.SequenceEqual(new[] { 2u, 7u, 300u }), "integer keys sort numerically");
        return 0;
    });
    env.Write(t => t.DropDb("users"));
    Check(!env.Read(t => t.HasDb("users")), "dropped");
});

Test("cursors", () =>
{
    using var env = Env.Open(path);
    env.Write(t =>
    {
        using var db = t.Db("words", DbFlags.Create);
        for (int i = 0; i < 1000; i++) db.Put($"w{i:D4}", $"value {i}");
    });
    using var r = env.ReadTxn();
    using var d = r.Db("words");
    using var c = d.Cursor();
    Check(c.First() && Encoding.UTF8.GetString(c.Key) == "w0000", "first");
    Check(c.Last() && Encoding.UTF8.GetString(c.Key) == "w0999", "last");
    Check(c.Prev() && Encoding.UTF8.GetString(c.Key) == "w0998", "prev");
    Check(c.Seek(B("w0500x")) && Encoding.UTF8.GetString(c.Key) == "w0501", "seek lands after");
    Check(c.SeekExact(B("w0777")) && Encoding.UTF8.GetString(c.Value) == "value 777" && Encoding.UTF8.GetString(c.Key) == "w0777", "seek exact");
    Check(!c.SeekExact(B("zzz")) && !c.IsValid, "seek exact miss");
    Check(c.First() && c.Skip(10) && Encoding.UTF8.GetString(c.Key) == "w0010", "skip");
    int n = 0;
    for (bool ok = c.First(); ok; ok = c.Next()) n++;
    Check(n == 1000 && !c.IsValid, "full scan");
    Check(d.Scan(B("w00")).Count() == 100, "prefix scan");
    Check(d.CountRange(B("w0100"), B("w0200")) == 100, "count range");
    Check(d.Stats().Entries == 1000, "stats");
    Throws(ErrorCode.ReadOnly, () => { using var x = env.ReadTxn(); using var y = x.Db("words"); y.Put("a", "b"); }, "write in a read transaction");
});

Test("cursor writes", () =>
{
    using var env = Env.Open(path);
    env.Write(t =>
    {
        using var db = t.Db("cw", DbFlags.Create);
        for (int i = 0; i < 10; i++) db.Put($"k{i}", "v");
        using var c = db.Cursor();
        Check(c.Seek(B("k3")) && c.Erase(), "erase under cursor");
        Check(c.Put(B("k3"), B("again")), "put through cursor");
        Check(db.GetString("k3") == "again" && db.Count == 10, "effects");
    });
});

Test("reserve fills in place", () =>
{
    using var env = Env.Open(path);
    env.Write(t =>
    {
        using var db = t.Db("big", DbFlags.Create);
        var span = db.Reserve(B("blob"), 1 << 20);
        for (int i = 0; i < span.Length; i++) span[i] = (byte)(i & 0xff);
    });
    env.Read(t =>
    {
        using var db = t.Db("big");
        Check(db.TryGet(B("blob"), out var v) && v.Length == 1 << 20 && v[12345] == (byte)(12345 & 0xff), "large value");
        return 0;
    });
});

Test("errors", () =>
{
    using var env = Env.Open(path);
    Throws(ErrorCode.NotFound, () => env.Read(t => { using var d = t.Db("missing"); return 0; }), "missing sub-database");
    Throws(ErrorCode.KeyTooLarge, () => env.Write(t => { using var d = t.Db(); d.Put(new byte[8192], B("x")); }), "huge key");
    var e = Assert(() => Env.Open(Path.Combine(work, "no/such/dir/x.nosql")));
    Check(e != null, "open failure throws");
    static NosqlException? Assert(Action a) { try { a(); return null; } catch (NosqlException x) { return x; } }
});

Test("durability, integrity, compaction, reopen", () =>
{
    var p = Path.Combine(work, "durable.nosql");
    using (var env = Env.Open(p, new EnvOptions { Sync = Durability.Lazy, SyncInterval = TimeSpan.FromMilliseconds(50), MaxDbs = 16 }))
    {
        for (int round = 0; round < 20; round++)
            env.Write(t => { using var db = t.Db(); for (int i = 0; i < 200; i++) db.Put($"k{i}", $"round {round}"); });
        env.Sync();
        env.CheckIntegrity();
        Check(env.Stats().LastTxn >= 20, "stats");
    }
    using (var env = Env.Open(p, new EnvOptions { ReadOnly = true }))
        Check(env.Read(t => { using var db = t.Db(); return db.GetString("k199"); }) == "round 19", "reopened read-only");
    var c = Path.Combine(work, "compact.nosql");
    Env.Compact(p, c);
    using (var env = Env.Open(c))
    {
        env.CheckIntegrity();
        Check(env.Read(t => { using var db = t.Db(); return db.Count; }) == 200, "compacted");
    }
    Check(new FileInfo(c).Length <= new FileInfo(p).Length, "compact is not larger");
});

Test("one writer, many readers across threads", () =>
{
    using var env = Env.Open(Path.Combine(work, "mt.nosql"));
    env.Write(t => { using var db = t.Db(); db.Put("n", "0"); });
    var stop = false;
    var readers = Enumerable.Range(0, 4).Select(_ => Task.Run(() =>
    {
        int reads = 0;
        while (!Volatile.Read(ref stop))
        {
            using var r = env.ReadTxn();
            using var db = r.Db();
            Check(int.Parse(db.GetString("n")!) >= 0, "reader");
            reads++;
        }
        return reads;
    })).ToArray();
    for (int i = 1; i <= 300; i++) env.Write(t => { using var db = t.Db(); db.Put("n", i.ToString()); });
    Volatile.Write(ref stop, true);
    Task.WaitAll(readers);
    Check(env.Read(t => { using var db = t.Db(); return db.GetString("n"); }) == "300", "final");
});

Console.WriteLine($"{checks} checks, {failures} failed");
return failures == 0 ? 0 : 1;
