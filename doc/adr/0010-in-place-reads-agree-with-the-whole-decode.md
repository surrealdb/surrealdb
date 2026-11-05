# ADR 0010: In-place reads agree with the whole decode

**Status:** Accepted · **Scope:** `surrealdb/expr/src/val/stored/`, `surrealdb/catalog/src/`
(`record.rs`, `packed/`), `surrealdb/core/src/exec/vm/row.rs`,
`surrealdb/core/src/exec/vm/row_packed.rs`, `surrealdb/core/src/exec/object_extract/`,
`surrealdb/core/src/exec/pre_decode_filter/`

**Relates to:** ADR 0009 (the 4.0 record format, whose byte grammars this ADR relies on), ADR 0007
(the VM reads rows through `RowReader`), ADR 0012 (verified generations admit rows unwalked).

## Context

A record is read in one of two ways. A *whole* read decodes the record into a `Value` and checks
everything in it. An *in-place* read works on the stored bytes and answers for one part of the
record: a field, a projection, a pre-decode predicate, or a top-k probe. Both must give the same
answer for every byte string a record can hold.

When they disagree on damaged bytes, a row's visibility depends on the shape of the query.
`SELECT a` can return a row that `SELECT *` refuses, and a pre-decode filter can drop a row
silently where a whole read would report it. The database then says different things about one row
depending on which fields the query names.

## Decision

**An in-place read of a v1 document refuses every row the whole decode refuses, whichever fields it
asks for.** The guarantee comes from the readers sharing one check, not from tests that compare
them.

**Documents reach in-place readers only checked.** The body of a v1 document (the stored-value
grammar of ADR 0009) reaches an in-place reader only as a `CheckedDocument` from the catalog. Its
constructor runs `stored::validate` over the whole body, and refuses a keyed document whose body
holds an `id` field, since a record whose `id` is its key leaves the `id` out. `stored::validate`
is the decoder run in a `Mode` that builds nothing (`Mode::Check`; the decoder uses `Mode::Build`).
Both modes walk the same bytes through the same code, so they accept the same bytes by
construction.

**A refused document is decoded whole.** A reader that meets a refused document falls back to the
whole decode, so the error a caller sees is the decode's error.

**Each in-place primitive refuses what the decode refuses.**

- Object lookups go through `OrderedObjectRef`, a type that proves its keys strictly ascend. It is
  made only by confirming the order. A nested object reached by a lookup is an `ObjectRef` again and
  is confirmed in turn before it is searched.
- `ValueRef::new` refuses a payload on a tag that is a whole value (`NONE` through `TRUE`, and the
  small integers).
- A `ValueRef` carries its depth, so a nested decode started from it applies the grammar's depth
  limit at the depth the value sits.

**Packed rows reach in-place readers only checked.** A packed row (ADR 0009) reaches an in-place
reader only as a `CheckedPackedRow` from `StoredRecord::checked_packed`. Its constructor runs the
same code as `PackedRow::decode`, instantiated in a mode that builds nothing (`Check`; the decode
is `Build`): `node`, `decode_slot`, `decode_remainder` and every per-kind decode function are
generic over the mode, each kind's check is its decode function instantiated twice, the rules over
a remainder read it through one view, and a key held both by a column and by its node's remainder
is found by one ordered cursor and refused where the node ends, as the decode refuses it. So the
check accepts exactly the rows the decode accepts and refuses the others with the decode's own
error. A field read is offered only on `CheckedPackedRow`, so it follows offsets the check proved.
The column-at-a-time reader of an unchecked `PackedRow` (`PackedRow::field`, `FieldPlan`) exists
only in test builds, to hold the shared read to the whole decode.

**A refused packed row is decoded whole.** The reader that meets a refused row calls the whole
decode on the same bytes, so the error a caller sees is the decode's.

**Revision-encoded records are the one exception.** Their in-place reads are described below.

## Revision-encoded records

A record written by a 3.x release is a `revision` value. The VM reads it in place through
`Layout::Revision` (`surrealdb/core/src/exec/vm/row.rs`), with the helpers in
`surrealdb/core/src/exec/object_extract/mod.rs`:

- The `data` field's bytes come from `rev2_record_data_bytes`, which reads a record of revision 2
  and slices its payload from the field's offset to the end. It checks the lengths it reads against
  the bytes it has. Any other revision, or a truncated envelope, is declined, and the record is
  decoded whole.
- Fields are found through `indexed_map_walker_from_object_bytes`, which builds its walker with
  `from_payload_unvalidated`. That walker does not validate the offset table or the key order of
  the object. Each offset is bounds-checked when it is used. An offset outside the payload is an
  error, and an error on a field read falls back to the whole decode.
- A lookup that finds no such key answers NONE from the walk. The rest of the record is not
  decoded.
- A field value read this way is not checked against the whole decode, and the rest of the record
  is not walked. The code assumes that storage checksums detect corruption before the bytes reach
  the reader (the comment on `indexed_map_walker_from_object_bytes`). The reader does not verify
  that assumption.

This is a known exception to the rule above. A revision record can answer a field read that the
whole decode would refuse, and a pre-decode filter can act on that answer. The exception ends for a
row when the row is rewritten in the v1 envelope, which an ordinary write does for the row it
touches (ADR 0009, Consequences). A document or a packed row is never read this way.

## Consequences

- Every in-place read of a document pays one O(bytes) walk of the document per row, and the check
  allocates nothing for the common kinds. Sets, regexes and geometries build their value to check
  it, since their encodings have rules a bare walk does not enforce.
- Every in-place read of a packed row pays one O(columns) check walk; the check allocates nothing
  for the kinds whose check builds nothing. A scan that reads fields of the row and settles the
  rows it keeps pays that walk and then the decode of each row it settles, which is what a
  document pays (its validation and its decode).
- The walk is the cost of not knowing that the stored bytes conform. The intended way to retire it
  is a verification pass over a table generation: once every row of a generation has been checked
  and the generation is marked verified, admission of a row of that generation can hand readers a
  document without walking it. The `CheckedDocument` constructor is the single place that decision
  is made, so the change stays local to it and to the marking. ADR 0012 takes this route: a
  generation is verified as it is minted, admission (`StoredRecord::admit_document`,
  `StoredRecord::admit_packed`) skips the walk for its rows, and the pre-decode filter judges them
  on the in-place path.
- The revision exception lasts as long as the revision rows it covers. Its scope does not grow:
  a new in-place reader of revision bytes must follow the rules of the revision section, and a
  reader of documents or packed rows must follow the decision.

## Alternatives rejected

- **Checking only the parts an in-place read uses.** The rows a query sees would then depend on the
  fields it names, which is the defect this ADR exists to remove.
- **A second checker for in-place reads.** Two checkers can disagree on the same bytes. One walk run
  in two modes cannot, because the mode changes only whether values are built.
