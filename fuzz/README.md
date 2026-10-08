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
