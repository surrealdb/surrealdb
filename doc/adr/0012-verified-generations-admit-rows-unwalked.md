# ADR 0012: Verified generations admit rows unwalked

**Status:** Accepted · **Scope:** `surrealdb/catalog/src/` (`layout.rs`, `record.rs`,
`packed/row.rs`), `surrealdb/datastore/src/tx.rs` (the mint),
`surrealdb/core/src/exec/vm/` (`row.rs`, `host.rs`, `row_frame.rs`, `row_packed.rs`,
`morsel/run.rs`), `surrealdb/core/src/exec/object_extract/`,
`surrealdb/core/src/exec/pre_decode_filter/`

**Relates to:** ADR 0010 (in-place reads agree with the whole decode), which this ADR completes
along the line its Consequences name; ADR 0009 (the 4.0 record format and its generations).

## Context

ADR 0010 makes every in-place read of a document or a packed row walk the row first, so that a
read refuses every row the whole decode refuses whichever fields it asks for. The walk is
O(bytes) for a document and, for a packed row, O(columns) plus the bytes of every string it
checks as UTF-8. It runs for every row a scan reads, whatever the row's fate: a selective
non-indexed scan pays it for every row it rejects. On crud-bench's documents (about 2 KiB each,
mostly text) it was the largest single cost of a filtered scan, which made such scans slower on
4.0 than on 3.3, whose in-place reads checked nothing.

ADR 0010 names the way out. The walk is the cost of not knowing that the stored bytes conform; a
generation whose rows are all known to conform can be marked verified, and admission of its rows
can then hand readers the row unwalked. The decision lives in one place, the admission of a row,
so the change stays local to it and to the marking.

## Decision

**A generation's descriptor carries the verification that vouches for its rows.**
`LayoutDescriptor::verified` (field 9, ignorable) holds a verification epoch. A reader trusts an
epoch of `VERIFIED_EPOCH` or later (`LayoutDescriptor::is_verified`), and
`TableLayouts::vouches_for(stamp)` answers whether the rows stamped with a generation are vouched
for. Stamp 0, an unlisted or refused generation, and a descriptor with no trusted epoch are not
vouched for.

**A generation is verified when it is minted.** The mint (`Transaction::mint_with`) writes
`verified: Some(VERIFIED_EPOCH)` into the descriptor of every generation it creates. This is sound
because, at the mint, no row carries the generation's number: the number has never been given to
another descriptor of the table (the `!gn` mark survives the table's removal), and a row is
stamped with a generation only by a record encoder writing into it, which writes only rows the
whole decode accepts. Every row the generation will ever hold is therefore one the whole decode
accepts, which is exactly what a verification pass over it would establish.

**Admission is still the one place the decision is made.** `StoredRecord::admit_document` and
`StoredRecord::admit_packed` take the layouts that admitted the row's generation. A row of a
generation those layouts vouch for is admitted unwalked: a document as it is, a packed row once its
structure parses (`PackedRow::new`, O(1)). Every other row is checked exactly as ADR 0010 requires
(`checked_document`, `checked_packed`). A reader with no layouts walks every row. `CheckedDocument`
and `CheckedPackedRow` are still made only by admission, and record whether they were walked
(`CheckedDocument::is_verified`).

**The pre-decode filter judges admitted rows on the in-place path.** ADR 0010 forbids rejecting a
row on a field before the row is validated, because the visibility of a damaged row would then
depend on the fields a query names. A row of a verified generation is one the whole decode
accepts, so that objection does not arise for it. An in-place scan (`scan_in_place` and the morsel
runner) now runs the scan's pre-decode filter ahead of its body for a packed row the admission
checked and for a document of a verified generation (`in_place_rejects`). A document of an
unverified generation is left to the body, which walks it once, rather than walked once for the
filter and again for the body. The filter's judgement is the WHERE the body refuses rows by first,
so a row it rejects is one the body would refuse. `EXPLAIN`'s `pre_decode_filter: yes` is
therefore true of both execution modes: the decoded scan applies the filter to every row, and the
in-place scan to every row it reads without a walk.

**A generation minted before this decision stays walked.** Its descriptor has no epoch, so its
rows are walked as ADR 0010 describes until they are rewritten into a newer generation, which an
ordinary write does for the row it touches. Rows of stamp 0 are always walked: they have no
descriptor to vouch for them.

## Consequences

- A filtered scan over a table whose rows were written by this build reads each row it rejects
  without walking it, on both layouts. On the crud-bench shape (1M rows, `number = 21`, 64
  concurrent scans) the walk drops out of the profile entirely.
- The guarantee changes form, not strength, for a verified generation. ADR 0010's in-place reads
  agree with the whole decode because both walk the same bytes; a verified generation's rows
  agree because the encoder wrote them and the decode accepts what the encoder writes. Both rest
  on the bytes being the bytes that were written: neither the walk once at verification nor the
  mint-time vouching detects damage that happens to bytes at rest afterwards, which is the domain
  of the storage engine's checksums. Every in-place read stays bounds-checked and fallible, so an
  unwalked damaged row reads as an error or a wrong field, never as a crash.
- An encoder that broke its promise would let rows of a verified generation answer in-place reads
  that the whole decode refuses. The epoch is the remedy: a release that finds such an encoder
  raises `VERIFIED_EPOCH`, and every generation minted under the old epoch is walked again.
- The field is ignorable. A release that does not know it skips it and walks every row, which is
  what it did before. A release that does not know it may still write rows into a verified
  generation; it writes them through its own record encoder, under the same promise.
- A verification pass over a generation minted before this decision (or over stamp 0) would let
  those rows skip the walk as well. It is not part of this decision: such rows exist only in stores
  written by 4.0 nightlies before it, and an ordinary write moves each row it touches into a
  verified generation.
- Tests that damage rows to hold the walk to the whole decode read through
  `TableLayouts::without_verification`, which vouches for nothing.

## Alternatives rejected

- **A separate verification key per generation.** It would keep descriptors byte-for-byte
  immutable, but every layouts load would read a second range, and the one moment a generation is
  known to be empty, its mint, is already the moment its descriptor is written.
- **A critical (even-numbered) field.** A release that refused the descriptor would refuse every
  row of the generation, which costs a mixed-version cluster its reads to buy nothing: a release
  that skips the field only walks.
- **Checking only the fields a query names.** Rejected by ADR 0010 for the reason it gives; a
  verified generation makes the question moot rather than answering it differently.
