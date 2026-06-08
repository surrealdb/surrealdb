# surrealdb-strand

A small-string-optimised immutable string type used throughout the SurrealDB value layer.

`Strand` backs string-shaped keys and values — `Value::String`, `Object` keys, `TableName`, `RecordIdKey::String` — and trades the mutability of `String` for three complementary storage strategies selected at construction time:

- **Inline** — short strings (≤ 23 bytes) stored directly on the stack, with no heap allocation.
- **Static** — a `&'static str` wrapper for compile-time known values. Construction is `const`, clone is a pointer copy, and drop is a no-op. Ideal for long literals (table names, reserved keywords, response keys) that would otherwise allocate. Build one with `Strand::new_static`.
- **Boxed** — dynamic long strings held in a `Box<str>`: one allocation per string, no atomic ops on construction or drop.

## Layout

`Strand` uses a custom 24-byte union layout on 64-bit platforms (the same size as `String`). The 24th byte is used as a discriminant tag:

- `0..=23` — the string is `Inline`, and the tag is the length.
- `254` — the string is `Static`.
- `255` — the string is `Boxed`.

This layout allows `as_str()` to be completely branchless, significantly improving the performance of equality and ordering comparisons for all variants.

## Usage

```rust
use surrealdb_strand::Strand;

// Short strings are stored inline (no allocation)
let s = Strand::from("hello");
assert!(s.is_inline());

// Compile-time constant — zero allocation, even for long strings
const KIND: Strand = Strand::new_static("geometry<multipolygon>");

// Dynamic long strings are heap-allocated in a Box<str>
let long = Strand::from("this string is longer than twenty-three bytes");
assert!(long.is_boxed());

// Format directly into a Strand, preferring inline storage
let key = Strand::from_display(42u32);
```

## License

This crate is part of SurrealDB and follows the same licensing terms.
