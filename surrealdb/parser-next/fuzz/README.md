# Parser fuzzing

`consistency` parses each input as a query and checks what must hold for any
input: no panic; an accepted query, printed as SurrealQL, parses back to the
same text; and a statement stream fed the input in two pieces yields what a
whole parse yields. Seed it from the language tests:

```
cd surrealdb/parser-next/fuzz
mkdir -p corpus/consistency
find ../../../language-tests/tests -name '*.surql' -size -8k -exec cp {} corpus/consistency/ \;
cargo +nightly fuzz run consistency -- -dict=surrealql.dict
```
