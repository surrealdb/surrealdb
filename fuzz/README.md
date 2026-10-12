# Fuzzing
Surrealdb maintains a set of fuzz testing harnesses that are managed by
[cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz).

To build and run the fuzzer we will need to;
- Install the nightly compiler
- Install cargo fuzz
- Build a fuzz friendly version of surrealdb with our harnesses

## Installing nightly
One of the key requirements for high-performance fuzzing is the ability
to collect code-coverage feedback at runtime. With the current stable
version of rustc we can't instrument our fuzz-harnesses with coverage feedback. 
Because of this we need to use some of the more bleeding edge features 
available in the nightly release. 

## Installing cargo-fuzz
Full details on the different install options are available, in the
[cargo-fuzz book](https://rust-fuzz.github.io/book/cargo-fuzz/setup.html).
but for the sake of brevity you can just install the basics with the
command below.

`cargo +nightly install cargo-fuzz`

## Building the fuzzers
Now that we've install cargo-fuzz we can go ahead and build our fuzzers.
If we want to build the executor fuzzer we can use the following command.
```
cargo +nightly fuzz build --fuzz-dir ./ fuzz_executor
````
This will build the fuzzer with debug info and with -03 or maximum optimizations.

When investigating an issue it might be more convenient to build the fuzzer without optimizations which will significantly speed up the build.
This will make fuzzing around 10 times slower but that is still plenty fast for replicating a found crash.
For building without optimizations add `-D`:
```
cargo +nightly fuzz build -D --fuzz-dir ./ fuzz_executor
````



## Running the fuzzer
Now that the fuzzer has successfully built we can actually run them. To
list the available fuzz harnesses we can use the command.
```
cargo +nightly fuzz list --fuzz-dir ./
```

Once we know what fuzzer (in this case fuzz_executor) we want to run we 
can it using the command;
```
cargo +nightly fuzz run --fuzz-dir ./ fuzz_executor
```

The previous command will run the fuzzer in libfuzzer's default mode,
which means as a single thread. If you would like to speed fuzzing
up we can make use of all cores, and use a dictionary file. e.g.
```
# -fork: Run N separate process fuzzing in parallel in this case we
#        use nproc to match the number of processors on our local
#        machine.
# -dict: Make use the fuzzer specific dictionary file.
cargo +nightly fuzz run --fuzz-dir ./ \
  #FUZZ_TARGET# -- -fork=$(nproc) \
  -dict=fuzz/fuzz_targets/fuzz_executor.dict
```

## Fuzzing the VM's execution of SurrealQL scripts
`fuzz_vm_executor` decodes each input into a short SurrealQL script over a
fixed, seeded dataset and runs it twice, each time on a fresh in-memory
datastore. It fails when the engine panics, and when any statement's value or
error differs between the two runs.

The generator names only deterministic builtins, gives every record it writes
an explicit id and breaks every `ORDER BY` tie on `id`, so a difference between
the runs is nondeterminism in the engine and not noise. Scripts run as the
owner, a database viewer, or a record user (`person:1`) against a table with
row-level permissions.

The toolchain is the one pinned in `rust-toolchain.nightly`. The comparison
needs no sanitizer, and on macOS AddressSanitizer reports a false positive in
`sysinfo` while the datastore starts, so build with `-s none`. Inputs are
decoded front to back, and the few bytes libFuzzer starts from all decode to
the same script, so seed the corpus with random inputs of a few hundred bytes:
```
cargo +nightly-2026-05-11 fuzz build -s none --fuzz-dir ./ fuzz_vm_executor
mkdir -p corpus/fuzz_vm_executor
for i in $(seq 1 500); do
  head -c 512 /dev/urandom > corpus/fuzz_vm_executor/seed-$i
done
cargo +nightly-2026-05-11 fuzz run -s none --fuzz-dir ./ fuzz_vm_executor -- \
  -fork=8 -ignore_crashes=1 -max_total_time=600 -max_len=1024 -timeout=30 -rss_limit_mb=4096
```
A failure prints the script, which statements differ, and each run's output.
Replay a saved input with
`FUZZ_PRINT=1 cargo +nightly-2026-05-11 fuzz run -s none --fuzz-dir ./ fuzz_vm_executor artifacts/fuzz_vm_executor/<crash-file>`.
Set `FUZZ_PRINT=1` to print every generated script and its results, which
shows how much of the generated SurrealQL parses and runs.

## Fuzzing the stored-value encoding
Three targets check the encoding records are stored in
(`surrealdb_expr::val::stored`) and the record envelope around it
(`surrealdb_catalog::StoredRecord`). Each compares values strictly: float bits
and NaN payloads, decimal scales, and the order of entries and elements.

- `fuzz_stored_decode` decodes arbitrary bytes. Neither the decoder nor the
  in-place reader may panic, nor the decoder allocate out of proportion to the
  input, a compiled regex aside. Bytes the decoder accepts must read the same
  in place, through every accessor, field lookup from any hint, and element
  read, and must write back: the value encodes, its encoding decodes to
  exactly the value, and encodes again to the same bytes.
- `fuzz_stored_roundtrip` builds a value of any storable kind from its input,
  edges included, and nesting either side of the depth limits. A value the
  encoder writes must decode to exactly itself and read the same in place; the
  encoder may refuse a value only for nesting that the record depth guard
  refuses too.
- `fuzz_record_envelope` reads its input as a stored record, in either
  encoding, or builds a record, metadata and key from it, and checks that each
  record written reads back exactly as written. A record is written in the v1
  envelope (`0xFF`, a header, a generation, a stored value) when it has no
  metadata or is an edge on the current adjacency layout with no statistics, and
  in the `revision` encoding for any other metadata. The target holds the writer
  to that rule: the marker byte is present exactly when the v1 envelope applies.
  A v1 row must also read back its header (edge, edge variant, keyed),
  generation 0 and metadata, and read the same in place; a `revision` row must
  decode to exactly the record written, and encode again to the same bytes.
  The reader of the `revision` encoding is bounded; in this target, arbitrary
  bytes in that encoding are checked only for not panicking. The allocation
  budget is applied by `fuzz_stored_decode`, to the stored-value decoder alone.
  The generator favours no metadata and an edge on the current layout, the two
  cases the v1 envelope holds, and the shapes next to them that fall back to
  `revision`.

`fuzz_stored_roundtrip` saves every encoding it writes to the directory
`FUZZ_STORED_SEEDS` names, which seeds `fuzz_stored_decode` with well-formed
bytes of every kind:
```
cargo +nightly-2026-05-11 fuzz build --fuzz-dir ./ fuzz_stored_roundtrip
mkdir -p corpus/fuzz_stored_roundtrip corpus/fuzz_stored_decode
cargo +nightly-2026-05-11 fuzz run --fuzz-dir ./ fuzz_stored_roundtrip -- \
  -max_total_time=600 -max_len=2048 -len_control=0 -timeout=20 -rss_limit_mb=4096
FUZZ_STORED_SEEDS=corpus/fuzz_stored_decode \
  cargo +nightly-2026-05-11 fuzz run --fuzz-dir ./ fuzz_stored_roundtrip -- \
  -runs=0 corpus/fuzz_stored_roundtrip
cargo +nightly-2026-05-11 fuzz run --fuzz-dir ./ fuzz_stored_decode -- \
  -max_total_time=600 -max_len=16384 -timeout=20 -rss_limit_mb=4096
```
The record bytes frozen in `surrealdb/core/src/kvs/compat/` (`STORED_RECORDS`,
and the `RECORD_*` arrays of earlier releases) seed `fuzz_record_envelope`
well, each written to a file after a zero byte, which makes the target read the
rest of its input as a stored record. `STORED_RECORDS` holds v1 rows, which
start with `0xFF`, and the `RECORD_*` arrays hold `revision` rows.
