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
