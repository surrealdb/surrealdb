# Shaped frames — a result format that sends each row's shape once

A result format complementary to JSON, CBOR and FlatBuffers, which are unchanged. A shaped stream
announces each row shape once (ordered columns: name, kind, nullability, children) and then sends
rows as values only: no field names and no per-value type tags.

Content type: `application/vnd.surrealdb.shaped`. WebSocket subprotocol: `shaped`.

## Goal

`SELECT *` and paged reads of SCHEMAFULL (packed) tables at memory-copy speed per row. The target
is under 60 ns/row of encoding above the scan for packed rows, and `SELECT * LIMIT 1000` bounded
by the scan and the fixed statement cost.

## Non-goals

- The wire is not the storage format. Rows are encoded from storage through a copy plan; storage
  is unchanged. Reasons: absent and `NULL` slots are not validated on read, so raw bytes can carry
  stale data; field `PERMISSIONS`, computed fields and projections make a row's content differ
  from what is stored; and a wire coupled to storage turns every storage change into a protocol
  break.
- No change to JSON, CBOR or FlatBuffers output.
- No gRPC, and no streaming over HTTP (HTTP has no `query_stream`).

## Scope (v1)

| Surface | Shaped content |
|---|---|
| HTTP `POST /sql` with `Accept: application/vnd.surrealdb.shaped` | The response body is the CBOR array of statement results; a row statement's `result` is a block |
| HTTP `POST /rpc`, `Content-Type: application/cbor`, `Accept: application/vnd.surrealdb.shaped` | Requests are CBOR; the response is the CBOR envelope; a `query` row result is a block |
| WebSocket subprotocol `shaped` | Requests and every response are CBOR (binary messages); `query` row results, `query_stream` `rows` frames and live notification payloads carry blocks |
| CBOR requests (WebSocket `cbor` and `shaped`, HTTP `/rpc` with `Content-Type: application/cbor`, the embedded engine's CBOR input) | A value in a request may be a block, which reads as the array of its rows ("Blocks in requests") |

Everything that is not a row list (errors, non-query methods, `INFO`, `RETURN`, scalars,
`SELECT ONLY`, DDL) is encoded exactly as the CBOR format encodes it. A client therefore needs one
block decoder plus the CBOR decoder it already has.

## Negotiation

There is no capability handshake RPC. The format is chosen the way every other format is chosen:

- WebSocket: the client offers `shaped` in `Sec-WebSocket-Protocol`; the server lists it in its
  protocols. A connection on `shaped` reads and writes CBOR, binary messages only, and a text
  message is rejected before parsing, as on `cbor`.
- HTTP `/rpc`: the request body is CBOR (`Content-Type: application/cbor`), and the response is
  shaped when `Accept` is `application/vnd.surrealdb.shaped`. `/rpc` otherwise requires `Accept`
  to equal `Content-Type`; the one admitted difference is CBOR in, shaped out.
- HTTP `/sql`: `Accept: application/vnd.surrealdb.shaped`.

A client that does not ask for `shaped` never receives a block. A block in a request needs no
negotiation: a build with the `shaped` feature accepts it in any CBOR request.

Shaped frames are built by the `surrealdb-core` feature `shaped`, which the server, the default
feature set and the edge bundle include and the lean wasm bundle leaves out. A build without it
does not list the subprotocol, reads `shaped` in a subprotocol, `Accept` or `Content-Type` as an
unsupported format and answers it as it answers any other, and refuses shaped rows asked of its
executor with an error per statement.

## Wire format v1

All integers are little-endian and unaligned: a reader never assumes any alignment. Offsets and
lengths are unsigned. Every size field is bounds-checked by the reader against the bytes it holds.

### Blocks in the CBOR envelope

A block is a CBOR tag `57` around a CBOR byte string (major type 2, definite length). A writer
writes the byte string's head in the four-byte form (`0x5a` and a big-endian `u32`, as CBOR
heads are), so a block's head is `d8 39 5a` and the length; a reader accepts any definite form. Tag 57 is unassigned in the IANA CBOR tags
registry; it sits next to the tags the CBOR format uses for files (55) and sets (56).
Its contents:

```
block    = version:u8  message*
version  = 0x01
message  = type:u8  length:u32  payload[length]
```

`length` is the number of payload bytes. A reader skips a message whose `type` it does not know.
The message types:

| type | name | payload |
|---|---|---|
| `0x01` | SHAPE | `shape_id:u32  shape` |
| `0x02` | BATCH | `shape_id:u32  row_count:u32  row[row_count]` |

A BATCH names a shape announced earlier in the same scope (below); a BATCH naming a shape id not
yet announced is an error. A SHAPE whose id was already announced in the scope replaces it for
the batches after it.

Where blocks appear and the scope their shape ids live in:

| Where | CBOR position | Shape scope |
|---|---|---|
| `query` response (HTTP `/sql`, `/rpc`, WebSocket) | a statement result's `result` value, when the statement's value is a list of rows | the block alone |
| `query_stream` `rows` frame | the `values` value of `{ stream: "rows", index, values }` | the whole stream (one `query_stream` request id); shapes announced in one frame hold for every later frame of that stream, of any statement |
| live notification | the notification's `result` value | the live query id, for the life of the subscription on that connection |

Everything else in the envelope — `begin`, `value`, `finished` and `end` stream frames, a
notification's `id`, `action`, `record` and `session`, errors — is CBOR as the CBOR format writes
it. The statement's terminal frame (`finished`) and the stream's `end` are unchanged: rows in
blocks are provisional until their statement's `finished`, exactly as rows in CBOR are.

An empty row list is a block holding only the version byte. Rows of one statement keep their
order across batches and frames.

### SHAPE

```
shape     = root:u8  presence_bits:u32  fixed_size:u32  var_count:u32  root_rest:u32
            column_count:u32  column[column_count]
column    = name_len:u16  name[name_len]  kind:u8  element:u8  flags:u8  presence:u32
            slot:u32  table_len:u16  table[table_len]  children:u32  rest:u32
```

- `root`: `0` OBJECT — each row is an object whose entries are the columns; `1` VALUE — each row
  is one value, held as a dynamic slice in var slot `root_rest`, and `column_count` is `0`.
- `presence_bits`: the bits of a row's presence region. `fixed_size`: the bytes of the fixed
  region. `var_count`: the number of variable slots.
- `root_rest`: for an OBJECT root, the var slot of the root's remainder, or `0xFFFFFFFF` when the
  root has none (the root is closed). For a VALUE root, the var slot of the value.
- Columns are listed depth-first, in key order within each object (byte order of the UTF-8): a
  column of kind `object` is followed immediately by its subtree, and `children` is the number of
  columns in that subtree, at every depth. `column_count` counts every column at every depth. A
  column that is not `object` has `children = 0`.
- `name`: the key's UTF-8, non-empty. Sibling names are distinct.
- `kind`: one of the kinds below. `element`: for kind `array`, the kind of every element (one of
  `bool`, `int`, `float`, `datetime`, `duration`, `uuid`, `string`, `bytes`, `decimal`); `0` for
  every other kind.
- `flags`: bit 0 `MAY_BE_ABSENT`, bit 1 `MAY_BE_NULL`; the other bits are zero. A column with
  neither flag is required and has no presence bits.
- `presence`: the first of the column's presence bits (below), or `0xFFFFFFFF` for a required
  column.
- `slot`: for a fixed kind, the byte offset of its slot in the fixed region; for `bit`, the index
  of its bit in the fixed region; for a variable kind, its var slot; for `object`, `0xFFFFFFFF`.
- `table`: for kind `record`, the table every value of the column names, or empty when the table
  is carried per value; empty for every other kind.
- `rest`: for kind `object`, the var slot of the node's remainder, or `0xFFFFFFFF` when the node
  is closed; for `datetime_ns` and `duration_ns`, the column's overflow var slot;
  `0xFFFFFFFF` for every other kind.

Slots, bits and var slots may lie in any order; a writer chooses them (a copy plan mirrors the
stored row, below). A reader refuses a shape whose `slot` + width passes `fixed_size` (or whose
bit passes `8 * fixed_size`), whose var slot is not below `var_count`, whose presence bits pass
`presence_bits`, whose names are not UTF-8, whose `children` run past `column_count`, whose
`element` does not match its kind, or whose object nesting passes the depth limit.

### Kinds

| code | kind | region | width | encoding |
|---|---|---|---|---|
| `0x01` | `bool` | fixed | 1 | `0x00` false, `0x01` true |
| `0x02` | `int` | fixed | 8 | `i64` |
| `0x03` | `float` | fixed | 8 | IEEE-754 binary64 bits |
| `0x04` | `datetime` | fixed | 12 | `secs:i64` since the Unix epoch, then `nanos:u32` (`< 2·10^9`; `10^9` and above only within a leap second, as the CBOR format's tag 12 carries one) |
| `0x05` | `duration` | fixed | 12 | `secs:u64`, then `nanos:u32` (`< 10^9`) |
| `0x06` | `uuid` | fixed | 16 | the 16 bytes in RFC 9562 order |
| `0x07` | `bit` | fixed | 1 bit | a bool: bit `slot % 8` (least significant first) of fixed byte `slot / 8`, set for true |
| `0x08` `0x09` `0x0a` | `i8` `i16` `i32` | fixed | 1, 2, 4 | a signed integer, read as an `int` |
| `0x0b` `0x0c` `0x0d` | `u8` `u16` `u32` | fixed | 1, 2, 4 | an unsigned integer, read as an `int` |
| `0x0e` | `u64` | fixed | 8 | an unsigned integer: an `int` up to `i64::MAX`, a decimal above |
| `0x0f` | `f32` | fixed | 4 | IEEE-754 binary32 bits, read as a `float` |
| `0x18` | `datetime_ns` | fixed | 8 | `i64` nanoseconds since the Unix epoch; `i64::MIN` (before the range) and `i64::MAX` (after it) say the value is in the overflow slot, 12 bytes as `datetime` |
| `0x19` | `duration_ns` | fixed | 8 | `u64` nanoseconds; `u64::MAX` says the value is in the overflow slot, 12 bytes as `duration` |
| `0x10` | `string` | var | — | UTF-8 |
| `0x11` | `bytes` | var | — | the bytes |
| `0x12` | `decimal` | var | — | the decimal's text, as CBOR tag 10 holds it |
| `0x13` | `record` | var | — | see below |
| `0x14` | `dynamic` | var | — | exactly one CBOR data item, as the CBOR format encodes the value |
| `0x16` | `array` | var | — | `count:u32`, then for a variable element kind (`string`, `bytes`, `decimal`) `ends[count]:u32` and the elements' bytes (element `k` is `bytes[ends[k-1]..ends[k]]`), for a fixed one the elements back to back in that kind's encoding |
| `0x20` | `object` | none | — | the node's columns, and its remainder if it is open |

An overflow slot is empty unless the fixed slot holds an escape value. Elements of an `array` are
never `NULL` or `NONE`; an array that holds one, or an element of another kind, is written in a
`dynamic` column.

A `record` slice is a key kind byte, then:

| key kind | then |
|---|---|
| `0x00` | `id:i64` |
| `0x01` | the string id's UTF-8 (the rest of the slice) |
| `0x02` | the 16 uuid bytes |
| `0x03` | the whole record id as one CBOR item (tag 8), table included |

For key kinds `0x00` to `0x02`, a column with an empty `table` has `table_len:u16 table[table_len]`
between the key kind byte and the id. Key kind `0x03` carries its own table.

A remainder slice (an open node's, or an open root's) is empty when the node has no keys beyond
its columns, or one CBOR map whose keys are text: the node's entries that have no column. Its
keys never collide with a column of the node that holds a value or `NULL`.

### Rows

```
row = presence[ceil(presence_bits / 8)]  fixed[fixed_size]  ends[var_count]:u32  var[L]
```

- Bit `b` of the presence region is bit `b % 8`, least significant first, of byte `b / 8`. A
  column's presence takes the bits its flags give it, from bit `presence`:
  - `MAY_BE_ABSENT` alone: one bit, set for a value, clear for absent;
  - `MAY_BE_NULL` alone: one bit, set for a value, clear for `NULL`;
  - both: two bits, `presence` the low one: `0b00` absent (the row does not hold the key), `0b01`
    a value, `0b10` `NULL`, `0b11` `NONE` (the row holds the key, and its value is `NONE`).
  
  A key whose value is `NONE` needs a two-bit column. Bits past `presence_bits` are zero.
- `ends[i]` is where var slot `i` ends, counted from the start of `var`; slot `i` is
  `var[ends[i-1] .. ends[i]]` with `0` before the first: the (offset, length) of the value. `L` is
  the last end (`0` when `var_count` is `0`). Ends do not decrease.
- A row's length is therefore known from its own bytes, and a batch is its rows back to back.

A column's value is read only when every object node enclosing it is present (state `value`, or
required). A column that is absent, `NULL` or `NONE`, and every column under a node that is not
present, has its fixed slot all zero and its var slot empty. Absent covers a key the row does not
hold and a field the caller may not select, and the two are the same bytes, as CBOR omits both:
the stream never says that a hidden value exists.

Which fields of a table a shape names follows the caller's select permission on each, decided in
physical planning: a field never visible to the caller (`PERMISSIONS NONE`, or a predicate that
folds to `false`) has no column and its name appears nowhere in the stream; a FULL field is a
column as declared; a field behind a row-dependent predicate is always a column, flagged
`MAY_BE_ABSENT`, and a row in which it is hidden holds it absent with its slot zero (or its var
slot empty), byte-identical to a row that does not hold the field. Permissions never cause a
shape to be re-announced.

Decoding a row into a value: an OBJECT row is an object holding, for each column whose state is
`value`, its name and its value, a `NULL` column as `NULL`, nothing for an absent column, plus
the entries of its remainder; an `object` column's value is built the same way from its children
and remainder. A VALUE row is its one dynamic value.

### Limits

| Limit | Value |
|---|---|
| Columns in a shape | 4096 |
| Object nesting in a shape | 32 |
| Shapes announced in one scope | 65 536 |
| Fixed region | 1 MiB |
| One block | the transport's message limit (the WebSocket and HTTP RPC body limits) |
| Dynamic CBOR depth | the CBOR format's recursion limit |

A reader refuses input past a limit; it never panics, and never reads outside the block.

### Versioning

The version byte opens every block. A change a v1 reader cannot skip (a new kind, a new row
layout) is a new version; a new message type is not, since a reader skips messages it does not
know. A server writes version 1 to a client that negotiated `shaped`.

### Framing and size

Batches follow the framing of the transport that carries them: a buffered `query` result is one
block; a `query_stream` statement's rows are framed by row count as every format's are
(`QUERY_FIRST_BATCH_RECORDS` rising to `QUERY_BATCH_RECORDS`), each `rows` frame carrying one
block. Frames are bounded by record count, not by encoded bytes, as on the CBOR subprotocol; the
WebSocket message limit applies unchanged.

## Blocks in requests

A client may send a block where a CBOR request holds a value: an RPC parameter, a query variable,
or any value nested in an object or array of one. Map keys are never blocks. It is accepted on
the WebSocket subprotocols `cbor` and `shaped`, on HTTP `/rpc` with `Content-Type:
application/cbor`, and on the embedded engine's CBOR input, and only there: a CBOR value decoded
anywhere else (a custom API body, `encoding::cbor::decode`, an export configuration, a
response) refuses tag 57. A build without the `shaped` feature refuses it everywhere.

**Principle.** A block in a request decodes, in the wire layer, to exactly the public value (an
array of row values) that some plain CBOR request could have carried. The executor, schema checks,
field types, `ASSERT`, permissions and the storage encoding see an ordinary value and treat it as
a CBOR-decoded one. A block never writes storage bytes and never skips a check, so a hostile block
can do no more than a hostile CBOR request, provided its decode is bounded and strict.

Rules, all enforced by the one CBOR conversion that every request value goes through, so a map
next to or around a block is held to the CBOR rules too (duplicate keys, integer range):

- **Scope.** Each block has a shape scope of its own, as a `query` response block has: shape ids
  announced in one block mean nothing in another.
- **Shape.** The block's contents are tag 57 around a byte string; any other item under tag 57 is
  refused.
- **No nesting.** A dynamic slice and a remainder are decoded as strict CBOR, which refuses tag 57.
- **Depth.** A block with `d` containers or tags around it, whose shape nests object columns `s`
  deep (`Shape::depth`), adds `s + 2` levels (the rows' array and object, then the objects). It is
  refused unless `d + s + 2` is below the request's recursion limit, and its dynamic slices decode
  with the limit that remains. A block therefore never reads to a value a plain CBOR request
  could not carry under the same limit.
- **Budget.** One budget serves every block of a request: `128 * request_bytes + 2^20` units, a
  unit being about a byte of memory. Plain CBOR decodes at most one value (about 64 bytes) from
  each byte of a request, so a block can materialise at most about twice what the densest CBOR
  request of the same size could. Decoding charges, before it allocates: 64 per row and per array
  element; `64 + 32 + name_len` per present column of any kind (a value, `NULL` or `NONE`,
  objects included), as each clones its name into the row; the column-level table's length per
  record value that takes its table from the column; the byte length of variable array elements;
  and 64 per byte of every dynamic and remainder slice. A shape is announced once but its names and tables are copied per row, so
  they are charged per row. A request that exceeds the budget is refused. The per-batch cap on
  rows of no bytes holds as well.
- **Records.** A `record` value held as a whole CBOR record id (key kind `0x03`) in a column whose
  shape names a table must name that table.
- **Everything else** the reader validates (UTF-8, ordering, bounds, datetime and duration
  ranges, the `MAX_*` limits) is unchanged. NaN and infinite floats are accepted, as in CBOR.

### Accepted CBOR tags

Every CBOR decode reads a datetime only as tag 12 `[seconds, nanos]`, a uuid only as tag 37 (16
bytes) and a duration only as tag 14 `[seconds, nanos]`. Tags 0 (a datetime as text), 9 (a uuid
as text) and 13 (a duration as text) are refused with an error naming the form to send instead. An
integer outside the signed 64-bit range, a map with a repeated key, and tag 6 (`NONE`) around
anything but `null` are refused rather than wrapped, merged or ignored.

## Layers

| Decision | Layer | Where |
|---|---|---|
| Which fields a caller can see, and whether rows take the copy plan | physical planning | a body that reads the row (permission layers, computed fields, projections) runs on the value path; only rows emitted untouched (`Pending::Emit` with `reads_row` clear) reach the copy plan, so a field whose select permission is not FULL for the caller never does |
| The shape of a packed row | physical (`exec::shaped`), from the generation's descriptor, a catalog fact the scan's `RowGate` resolves | a pure function of the descriptor: one column per descriptor column, kinds mapped as below |
| The seed shape of a statement's value-path rows | physical planning: derived when a statement is compiled for its request (`exec::vm::compile::seed_statement`, `exec::shaped::seed_shape`) from the table's field definitions as the transaction reads them and whether the principal it is compiled for has its reads checked, and carried on the program; units compiled while a program runs get none, and the run reads no catalog | statically never visible (`PERMISSIONS NONE`, or a predicate that folds to `false`) → the column is not in the shape; FULL (or a caller whose permissions are not checked) → as declared; a row-dependent predicate → in the shape as `MAY_BE_ABSENT` |
| The copy plan (packed generation → shape) | physical (`exec::shaped`), built once per (table, generation) per statement, at the first row of that generation, from the descriptor and the statement's seed shape | one block copy of the fixed region, then the ops a row needs: clear what the row does not hold, splice var slices (in runs), write overflow, array, record and dynamic slices |
| Running the copy plan per row | core `RowFrame` (`RowFormat::Shaped`), through the packed row's column reads (`PackedRow::column_state`, `column_value`, `column_bytes`, `column_remainder`, `fixed_bytes`), which share the bounds-checked slot reads of `PackedRow::visit` | no `Value` built; the record id from the key |
| Value-path rows (permissions, computed fields, projections, schemaless documents) | core `RowFrame` | encoded from the `Value` into the stream's current shape when it fits, else into a shape derived from the row (below) |
| Shape ids, SHAPE messages, blocks | the transport (`rpc::format::encoded` for buffered responses, the WebSocket stream driver for `query_stream`, the WebSocket connection for notifications) | rows carry the shape they were encoded in; the writer announces it in the scope the first time |
| The reference decoder | `surrealdb-types` (`shaped`) | block → public `Value`; dynamic slices go through a CBOR decoder the caller supplies |

The copy plan is built when the first row of a generation reaches the frame, from the descriptor
the scan's `RowGate` already resolved for the snapshot it reads; the run reads nothing more of the
catalog. It is not built when the program is compiled: which generations a scan meets is a fact
of the rows, and a table's layouts can list generations the statement never reads.

### Packed descriptor → shape

The shape of a packed generation mirrors the stored row's fixed region: its presence bits are the
stored presence bits, its bools are the stored bool bits, and every slot the wire holds as stored
lies at the stored offset (less the presence bytes) with the stored width. A row's presence and
fixed regions are therefore one copy of the `fixed_size` stored bytes the descriptor names.

| Physical kind | Wire kind |
|---|---|
| `Bool` | `bit` (the stored bool bit) |
| `I8` `I16` `I32` `I64` | `i8` `i16` `i32` `int` |
| `U8` `U16` `U32` `U64` | `u8` `u16` `u32` `u64` |
| `F32` `F64` | `f32` `float` |
| `Uuid` | `uuid` |
| `DatetimeWide` | `datetime` |
| `Datetime` | `datetime_ns` (an escaped slot's value, from the trailer, in its overflow slot) |
| `Duration` | `duration_ns` (likewise) |
| `Decimal16` `DecimalVar` | `decimal` |
| `String` | `string` |
| `Bytes` `Fixed(n)` | `bytes` |
| `RecordKey` | `record` (`table` set when the column names one table) |
| `Value` | `array` where the statement's seed shape declares the field an array of one plain kind, `dynamic` otherwise (a `NULL` held in the slice is CBOR `null`) |
| `Point` `LiteralCode` | `dynamic` |
| `Object` | `object` |
| a `Remainder` | the node's `rest` slot (`dynamic` map) |

The root gets an `id` column of kind `record` with the table set, whose var slot is the last. A
column's flags follow its stored presence: `Optional` → `MAY_BE_ABSENT`, `Nullable` →
`MAY_BE_NULL`, `OptionalNullable` → both, `Required` → none; the stored bit conventions are the
wire's.

### The copy plan and stale bytes

A row is written as: the stored presence and fixed regions copied as one block; every stored slot
the wire holds in another form (`Decimal16`, `Fixed`, `Point`, `LiteralCode`) zeroed; then, for
each column the row does not hold (its own presence, or an enclosing node's), its fixed slot
zeroed and its bool bit cleared, and under a node the row does not hold its presence bits cleared
too. So no byte of a column the row does not hold reaches the wire, whatever the stored row's
unused bytes hold. Variable slices are spliced: consecutive stored slices of string and bytes
columns in consecutive wire slots are copied as one run, after checking that every member the row
does not hold has an empty stored slice (a row that breaks this falls back to the whole decode);
every other slice is written alone. An escaped datetime or duration keeps its escape value in the
fixed slot and has its value, read from the trailer, written to its overflow slot. A stored array
of a field the seed declares an array of one plain kind is written natively, and fails the row
(which then takes the whole decode and the value path) if an element is of another kind.

### Value-path shapes

A value-path row is encoded into the frame's current shape when it fits: an object row fits when
every key it holds has a column or the node is open, and every value matches its column's kind
(a missing key needs `MAY_BE_ABSENT`, `NULL` needs `MAY_BE_NULL`, `NONE` needs both). A value fits
a stored-width column when that width holds it exactly.
A row that does not fit is encoded into the join of the current shape and the row: missing
columns are added (`MAY_BE_ABSENT | MAY_BE_NULL`), a kind conflict makes the column `dynamic`, a
required column a row leaves out gains `MAY_BE_ABSENT`. A record column whose values name more
than one table carries the table per value. Nested objects become `object` columns to depth 4,
`dynamic` below. After 16 joins in one frame, or past 256 columns, the next shape is open with no
columns (an OBJECT root whose rows are all remainder), so heterogeneous rows cannot make every
row announce a shape. A row that is not an object is a VALUE row.

When the statement has a seed shape (above) and an object row's `id` names the seed's table, the
row's shape starts from the seed: every visible declared field is a column from the first row,
whether or not that row holds it.

## Security invariants (SECURITY_GUIDE §2, §6)

- A field the caller cannot select is never present in a batch, by value or by stale bytes; a
  field hidden by a row-dependent permission is absent in that row, byte-identical to a row that
  does not hold the field.
- A field whose select permission is statically never visible to the caller is not named in any
  shape of the stream.
- Every value in a batch was read through a bounds-checked accessor or the value path; nothing is
  copied from a storage region the descriptor does not name, and no slot of a column absent or
  `NULL` in the row is copied.
- Frame limits and the streaming protocol (provisional rows until `finished`, cancellation,
  per-connection registries) apply unchanged. Shape scopes are per stream and per connection's
  live query; nothing is shared between connections.
- The reference decoder bounds every offset and length, caps nesting and dynamic CBOR depth, and
  never panics on any input.

## Acceptance

- Reference decoder in Rust (`surrealdb-types`) turning blocks back into `Value`.
- Differential: decode(shaped) == decode(CBOR) for every field kind, `NONE`/`NULL`, nested
  objects, arrays, projections, field `PERMISSIONS` as owner and as a record user, computed
  fields, shape changes mid-stream (ALTER, pending MIGRATE), schemaless and dynamic columns,
  non-row results, live notifications including DIFF, an object entry whose value is `NONE`
  (`SELECT NONE AS x`).
- Permissions: a row-dependent predicate as two record users, each seeing only their own values,
  with hidden slots zero in the raw batch bytes; absent-by-permission byte-identical to a field the row does not hold; a
  statically hidden field's name nowhere in the stream's bytes.
- Bit flips and truncations of blocks: the decoder never panics.
- Allocations ≤ 0.02 per row for packed rows; ns/row and `LIMIT 1000` µs against CBOR, measured
  in the same run.
- Wasm lean and edge sizes measured; gating decided after.

## Owner decisions (2026-10-17)

1. Layout: custom row-major. Arrow may come later as a separate export format.
2. Scope of v1: query results over HTTP and WebSocket (including `query_stream`), plus live
   notifications over WebSocket. No gRPC.
3. Wasm: the shaped encoder is behind the `shaped` feature: in the edge bundle, out of the lean
   one.
4. Rows on the value path are encoded from the `Value` into the same format.
5. Field permissions: statically never visible → not in the shape; FULL → as declared;
   row-dependent → in the shape as may-be-absent, hidden values absent and zeroed, identical to a
   field the row does not hold (as CBOR omits it). Naming a row-dependent field is accepted. A
   value that is `NONE` has a presence state of its own.
