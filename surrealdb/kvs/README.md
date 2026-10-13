# surrealdb-kvs

Common types and traits shared by every SurrealDB key-value store backend: the
`Transactable` transaction trait, the `TransactionBuilder` datastore
abstraction, the raw `Key`/`KeyRange`/`Val` byte types, and the shared error,
configuration, cursor, and timestamp machinery.

> [!WARNING]
> This crate is SurrealDB internal API. It does not adhere to SemVer and its
> API is free to change and break code even between patch versions. If you are
> looking for a stable interface to the SurrealDB library please have a look at
> the [Rust SDK](https://crates.io/crates/surrealdb).

## Read views, range cursors, estimate and split

A backend that implements none of this is correct. `Transactable::snapshot`
answers `None`, and `estimate` and `split` are answered from `scan`, `keys`
and `keysr` by defaults that hold for any backend.

**`estimate(rng, version)`** answers `Estimate { keys, bytes, exact }`. A range
of fewer than `ESTIMATE_BUDGET` keys is counted and `exact`. A larger one is
extrapolated from its first `ESTIMATE_BUDGET` keys by the share of the range
they span (`position_fraction`), and is not `exact`.

**`split(rng, target, version)`** answers boundary keys that divide `rng` into
`Parts(n)` parts, or into parts of about `Bytes(b)` key and value bytes. The
boundaries are strictly inside the range and strictly ascending, so the parts
they cut tile the range exactly: every key lies in exactly one part, in either
direction. A range of one key, or of keys too close to divide, answers no
boundaries. The default finds the first and last key with one seek each and
cuts between them with `split_range`. A backend that knows its own layout
(its pages, its SSTs) overrides it to cut where the data divides evenly.

**`snapshot()`** answers an owned, shareable `Snapshot`: a read view of what
the transaction reads at that moment, for code that does not hold the
transaction. A backend answers `None` whenever it cannot overlay the writes the
transaction has buffered, as RocksDB does for a writable transaction. A view
upholds:

- *Same view.* Commits made after the view was taken, and writes the
  transaction makes later, are invisible to it.
- *Concurrent.* Cursors opened from one view advance on different threads and
  share no lock on the read path.
- *Keep-alive.* `commit` and `cancel` wait until every cursor opened from the
  view is dropped. Once the transaction has finished, `open` fails with
  `Error::TransactionFinished`; cursors already open stay readable.

A `RangeCursor` is re-aimed with `aim` across ranges and directions without
giving up its iterator. A backend implements `for_each` (visit in place) and
`fill` (copy into a caller-owned `Page`); there is no default, because a cursor
that cannot leave an entry unread cannot honour a page's size.

- *Consume rule.* An entry handed to a visitor is consumed whatever the visitor
  answers. A `Break` stops the chunk after that entry, and `resume()` names the
  entry after it; `resume().remainder(range, dir)` is the range still to read,
  and `None` once nothing is. `fill` never takes an entry its page cannot hold:
  the entry stays unread, and one larger than an empty page is delivered alone
  in a page grown for it.
- *`narrow`.* `narrow(end)` shortens the range still to be read without
  re-seeking: forward, `end` becomes the exclusive end; backward, the inclusive
  start. A bound outside the range still to be read is an error.
- *Keep-alive.* A cursor holds the transaction alive; drop it before finishing
  the transaction.

A wrapper that implements `Transactable` forwards `snapshot`, `estimate` and
`split` explicitly, as it does every other method with a default; a wrapper
that does not forward `snapshot` hides the view. The contract is exercised for
every backend by the `read_view` module of `surrealdb-kvs-test`.
