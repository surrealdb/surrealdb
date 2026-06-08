# SurrealDB Collections

Sorted-`Vec`-backed `VecMap<K, V>` and `VecSet<T>` containers used inside
`surrealdb-core`. They are wire-compatible stand-ins for `BTreeMap` and
`BTreeSet` at the small-to-medium sizes that dominate the SurrealDB value
tree (object fields, sets of record ids, etc.), trading insertion cost for
lower memory overhead, faster iteration and cloning, and cheap construction
from already-sorted data.

> **Note.** This crate is part of SurrealDB's **internal API**. It is
> published to crates.io because the workspace requires it for
> `surrealdb-core`, but it offers **no stability guarantees** between
> releases. For a stable interface to SurrealDB, use the
> [Rust SDK](https://crates.io/crates/surrealdb).

## When to use it

Reach for `VecMap` / `VecSet` instead of `BTreeMap` / `BTreeSet` when:

- You typically have a handful of entries. Most lookups in the SurrealDB
  value tree hit collections with fewer than 64 keys, where a forward linear
  scan beats any tree walk on cache locality.
- The collection is built once and then read or serialised many times.
- You need byte-identical `revision` and `storekey` encodings to those of
  `BTreeMap` / `BTreeSet`. Both directions decode interchangeably, so the
  on-disk format is unchanged when migrating callers.

Stick with `BTreeMap` / `BTreeSet` (or `HashMap` / `HashSet`) when the
collection is large and write-heavy: insertions and removals here are
O(n) because they shift the underlying `Vec`.

## Highlights

- **Single sorted, deduplicated `Vec`.** `#[repr(transparent)]` over the
  backing buffer, no allocations beyond the `Vec` itself, and
  `#![forbid(unsafe_code)]`.
- **Hybrid search.** Look-ups use a linear scan up to 64 entries and
  `binary_search_by` above it; the boundary is tuned against the hot path
  in the SurrealDB `Object` / `Set` types.
- **Wire-format parity with `BTreeMap` / `BTreeSet`.** Encodings produced
  via [`revision`] and [`storekey`] are byte-for-byte identical, so
  persisted data can be read back into either type without re-encoding.
- **Cheap construction from sorted input.** `from_sorted_vec_unchecked`
  bypasses the sort + dedup pass and is the right choice when converting
  from a `BTreeMap`, a revision-decoded payload, or any other
  already-ordered source.
- **`BTreeMap`-shaped API.** `entry`, `retain`, `append`,
  `first_key_value` / `last_key_value`, `into_values`, and `Index<&K>` are
  all present; `VecMap` also has `merge_sorted_prefer_rhs` and `push` for
  the linear-merge fast paths used when combining already-ordered streams.

## Quick tour

```rust
use surrealdb_collections::{VecMap, VecSet};

let mut m: VecMap<&str, i32> = VecMap::new();
m.insert("b", 2);
m.insert("a", 1);
assert_eq!(m.iter().map(|(k, _)| *k).collect::<Vec<_>>(), ["a", "b"]);

let s: VecSet<u32> = (0..5).collect();
assert!(s.contains(&3));
let evens: VecSet<u32> = s.iter().copied().filter(|n| n % 2 == 0).collect();
let _union = s.union(&evens);
```

## Invariants and `_unchecked` APIs

The sort + uniqueness invariant is the entire correctness story:

- `VecMap` keys and `VecSet` elements are in **strictly ascending** `Ord`
  order with no duplicates.
- `from_sorted_vec_unchecked`, `push`, and `VecMap::append` only enforce
  the invariant via `debug_assert!`. Violating it in release builds is
  logically unsafe even though the crate contains no `unsafe` code:
  lookups, equality, and the encoded byte stream all silently observe the
  broken order.
- The `storekey::BorrowDecode` and `revision::DeserializeRevisioned` impls
  require the wire stream to enumerate keys / elements in **strictly
  ascending** order (as produced by `Encode` / `SerializeRevisioned` for
  these types or their `BTreeMap` / `BTreeSet` counterparts). Violations
  return `storekey::DecodeError::InvalidFormat` or
  `revision::Error::Deserialize` in **all** builds, including release.

## Benchmarks

Criterion benchmarks comparing `VecMap` / `VecSet` to `BTreeMap` /
`BTreeSet` and `HashMap` / `HashSet` across small, medium, and large tiers
(plus the storekey- and revision-deserialisation paths) live in
`benches/collections.rs`:

```sh
cargo bench -p surrealdb-collections --bench collections
```

[`revision`]: https://docs.rs/revision
[`storekey`]: https://docs.rs/storekey
