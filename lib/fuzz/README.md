# Fuzzing
Surrealdb maintains a set of fuzz testing harnesses that are managed by
[cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz).

To build and run the fuzzer we will need to;
- Install a specific version of the nightly compiler
- Install cargo fuzz
- Build a fuzz friendly version of surrealdb with our harnesses

## Installing nightly
One of the key requirements for high-performance fuzzing is the ability
to collect code-coverage feedback at runtime. With the current stable
version of rustc we can't instrument our fuzz-harnesses with coverage feedback. 
Because of this we need to use some of the more bleeding edge features 
available in the nightly release. 

Unfortunately for us the nightly release is a little unstable and there
was a [bug](https://github.com/rust-lang/rust/issues/110475) in the 
latest version of the nightly compiler that prevents use from compiling
some of surrealdb's depdendencies. To workaround this issue we've carefully
picked a version of the nightly compiler that works with both cargo-fuzz
and our dependencies. This version is `nightly-2023-04-21`. To install
this version we simply need to run;

`rustup install nightly-2023-04-21`

## Installing cargo-fuzz
Full details on the different install options are available, in the
[cargo-fuzz book](https://rust-fuzz.github.io/book/cargo-fuzz/setup.html).
but for the sake of brevity you can just install the basics with the
command below.

`cargo +nightly-2023-04-21 install cargo-fuzz`

## Building the fuzzers
Now that we've install cargo-fuzz we can go ahead and build our fuzzers.
```
cd lib  
# -O: Optimised build
# --debug-assertions: Catch common bugs, e.g. integer overflow.
cargo +nightly-2023-04-21 fuzz build -O --debug-assertions
````

## Running the fuzzer
Now that the fuzzer has successfully built we can actually run them. To
list the available fuzz harnesses we can use the command.
```
cargo +nightly-2023-04-21 fuzz list
```

Once we know what fuzzer (in this case fuzz_executor) we want to run we 
can it using the command;
```
cargo +nightly-2023-04-21 fuzz run -O --debug-assertions fuzz_executor
```

The previous command will run the fuzzer in libfuzzer's default mode,
which means as a single thread. If you would like to speed fuzzing
up we can make use of all cores, and use a dictionary file. e.g.
```
# -fork: Run N seperate process fuzzing in parralell in this case we
#        use nproc to match the number of processors on our local
#        machine.
# -dict: Make use the fuzzer specific dictionary file.
cargo +nightly-2023-04-21 fuzz run -O --debug-assertions \
  fuzz_executor -- -fork=$(nproc) \
  -dict=fuzz/fuzz_targets/fuzz_executor.dict
```