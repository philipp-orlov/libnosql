# Fuzzing

Four libFuzzer targets, built with clang under the address and undefined
behaviour sanitizers when `NOSQL_BUILD_FUZZERS` is on:

| target | drives |
|---|---|
| `fuzz_ops` | well-formed stores against a `std::map` model: every tree flavour and page size, nested and optimistic transactions, spills, shrinks, reopens and compactions |
| `fuzz_pages` | pages that pass their checksum and lie about their layout (`FUZZ_SUFFIXES=1` for prefix-compressed leaves) |
| `fuzz_power` | power cuts at every point of a workload the input picks |
| `fuzz_replication` | shipping and applying deltas through damaged, swapped and reordered segments and bundles |

`corpus/<target>/` holds each target's seeds: a set cover of what earlier
campaigns found, so a run starts from their coverage rather than from
nothing. Give a run a scratch directory first, which is where libFuzzer
writes what it finds, and the seeds after it:

```sh
cmake -S . -B build-fuzz -DCMAKE_CXX_COMPILER=clang++ -DNOSQL_BUILD_FUZZERS=ON
cmake --build build-fuzz
mkdir -p /tmp/ops && build-fuzz/fuzz/fuzz_ops -max_len=8192 /tmp/ops fuzz/corpus/ops
```

To fold a campaign's finds back into the seeds, merge into an empty
directory and replace the old one with it:

```sh
mkdir /tmp/seeds && build-fuzz/fuzz/fuzz_ops -set_cover_merge=1 /tmp/seeds fuzz/corpus/ops /tmp/ops
rm -r fuzz/corpus/ops && mv /tmp/seeds fuzz/corpus/ops
```

A crash an input finds is fixed together with a regression test in the
suite that covers it; the input itself can join the seeds.
