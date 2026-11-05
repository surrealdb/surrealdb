# ADR 0011: Sized number kinds

**Status:** Accepted · **Scope:** `surrealdb/types/src/` (`value/number.rs`, `kind.rs`,
`traits/surreal_value.rs`, `shaped/`), `surrealdb/parser-next/src/`, `surrealdb/expr/src/`
(`val/number.rs`, `val/stored/`, `val/value/convert/`, `val/key_encoding.rs`, `expr/kind.rs`,
`parser/`), `surrealdb/catalog/src/` (`packed/`, `layout_derive.rs`), `surrealdb/runtime/src/`,
`surrealdb/core/src/` (`exec/`, `function/builtin/`, `idx/`, `rpc/format/`),
`surrealdb/server/src/ntw/`

**Relates to:** ADR 0007 (the bytecode VM executes the physical plan), ADR 0008 (named record-id
components), ADR 0009 (the 4.0 record format), ADR 0010 (in-place reads agree with the whole
decode).

## Context

SurrealQL's numeric kinds are `int` (`i64`), `float` (`f64`), `decimal`, and the union kind
`number`. Schema-constrained workloads, compact packed-row storage, SQL front ends (`pgwire`,
`mywire`, `sqlplan`), and typed client bindings need fixed-width integer and floating-point kinds
whose width is part of the runtime value rather than a schema-only check. At the same time,
existing persisted bytes (`revision`, the 4.0 stored-value grammar, packed rows, and index keys)
and unnegotiated wire formats (`cbor`, `flatbuffers`, `json`) must keep their exact byte layout and
wire compatibility with released 3.x peers.

## Decision

### 1. Kinds and runtime values

**Nine sized number kinds join `int`, `float`, and `decimal` as first-class values.** Both
`surrealdb_types::Kind` and `surrealdb_expr::Kind` carry `I8`, `U8`, `I16`, `U16`, `I32`, `U32`,
`U64`, `F16`, and `F32`, and both `surrealdb_types::Number` and `surrealdb_expr::val::Number` carry
the corresponding payload variants (`i8`, `u8`, `i16`, `u16`, `i32`, `u32`, `u64`, `half::f16`,
`f32`) after `Int(i64)`, `Float(f64)`, and `Decimal(Decimal)`:

| Kind text | `Kind` / `Number` variant | Rust type | Domain |
|---|---|---|---|
| `i8` | `I8` | `i8` | $-128 \dots 127$ |
| `u8` | `U8` | `u8` | $0 \dots 255$ |
| `i16` | `I16` | `i16` | $-32\,768 \dots 32\,767$ |
| `u16` | `U16` | `u16` | $0 \dots 65\,535$ |
| `i32` | `I32` | `i32` | $-2^{31} \dots 2^{31}-1$ |
| `u32` | `U32` | `u32` | $0 \dots 2^{32}-1$ |
| `int`, `i64` | `Int` | `i64` | $-2^{63} \dots 2^{63}-1$ |
| `u64` | `U64` | `u64` | $0 \dots 2^{64}-1$ |
| `f16` | `F16` | `half::f16` | IEEE 754 binary16 |
| `f32` | `F32` | `f32` | IEEE 754 binary32 |
| `float`, `f64` | `Float` | `f64` | IEEE 754 binary64 |
| `decimal` | `Decimal` | `rust_decimal::Decimal` | 96-bit mantissa, scale $\le 28$ |
| `number` | `Number` (`Kind` only) | any `Number` | union of all twelve numeric kinds |

`i64` and `f64` are surface aliases for `Kind::Int` and `Kind::Float`; both render as `int` and
`float` so catalog kind text, `INFO`, and exports have one canonical spelling. Every numeric kind is
a subtype of `Kind::Number`. `size_of::<Number>()` is 24 bytes and `size_of::<Value>()` is
unchanged.

**Value-only sites project through `Wide`.** `Number::wide(self) -> Wide` maps every number
losslessly onto the three wide representations (`Wide::Int(i64)` for `I8`, `U8`, `I16`, `U16`,
`I32`, `U32`, `Int`, and `U64` values $\le \text{i64::MAX}$; `Wide::Decimal(Decimal)` for `Decimal`
and `U64` values $> \text{i64::MAX}$; `Wide::Float(f64)` for `F16`, `F32`, and `Float`). Code that
depends on numeric value rather than width matches on `n.wide()`; code that depends on width lists
all twelve `Number` variants without a wildcard arm.

### 2. Literals and rendering

**Width suffixes are part of the numeric literal grammar.**

- Integer suffixes `i8`, `u8`, `i16`, `u16`, `i32`, `u32`, `i64`, `u64` require integer digits
  (with optional `_` separators); a fractional part or exponent with an integer suffix is a parse
  error.
- Float suffixes `f16`, `f32`, `f64`, `f` accept integer digits, an optional fractional part, and an
  optional exponent.
- `i64` produces `Number::Int` and `f64` produces `Number::Float`.
- A literal whose value lies outside its suffix's domain, or a float literal that overflows to
  $\pm\infty$ in its target width, is a parse error.
- Leading `-` in literal position is folded before range checking so minimum signed bounds
  (`-128i8`, `-32768i16`, `-2147483648i32`, `-9223372036854775808i64`) parse as single literals.

**`ToSql` and `Display` round-trip every kind and value.**

- Sized integers render as decimal digits followed by their suffix (`5u8`, `-3i16`,
  `18446744073709551615u64`). `Int` renders without a suffix.
- Finite `F16` and `F32` values render as the shortest decimal representation that parses back to
  the same IEEE bits, followed by `f16` or `f32`. `F16` finds the shortest decimal that round-trips
  to the same `half::f16` bits before falling back to `f32` formatting.
- Non-finite `F16` and `F32` values render as `<f16>NaN`, `<f16>Infinity`, `<f16>-Infinity`,
  `<f32>NaN`, `<f32>Infinity`, `<f32>-Infinity` so they re-parse to the same kind and value.

### 3. Equality, ordering, hashing, arithmetic, coercion, and casts

**Equality, ordering, and hashing operate by numeric value across all twelve kinds.** `Ord`, `Eq`,
and `Hash` compare and hash `Number` values through their widened numeric value: `5u8 == 5 == 5.0f
== 5dec`, every NaN compares equal to every other NaN and orders above $+\infty$, and equal numbers
of any kind produce identical hashes. `GROUP BY`, `DISTINCT`, sets, and `UNIQUE` indexes treat equal
numbers of different widths as one key.

**Binary arithmetic promotes operands through `NumberKind::join`.** Both runtime evaluation and
static kind inference compute the result kind of binary `+`, `-`, `*`, `%`, and `**` via
`NumberKind::join`:

- **Two integer kinds:** the narrowest integer kind whose range contains both operand ranges, or
  `Decimal` when one operand is `U64` and the other is signed (`I8`, `I16`, `I32`, `Int`).
- **An integer kind and a float kind:** the wider of the float kind and the narrowest float kind
  that represents the integer kind's full range (`I8`/`U8` $\to$ `F16`, `I16`/`U16` $\to$ `F32`,
  `I32`/`U32`/`Int`/`U64` $\to$ `Float`).
- **Two float kinds:** the wider float kind (`F16` $<$ `F32` $<$ `Float`).
- **Any kind with `Decimal`:** `Decimal`.

Integer operations compute in `i128` (with checked exponentiation for `**`) and range-check into the
joined result kind; checked operators (`TryAdd`, `TrySub`, `TryMul`, `TryPow`, `TryNeg`) return an
overflow error when an integer or decimal result lies outside the result kind's range, while float
operations follow IEEE 754 and overflow to $\pm\infty$. Division `/` on two integer kinds performs
truncating integer division in the joined integer kind (returning an overflow error on `MIN / -1`
and a division-by-zero error on `/ 0`); float division (`try_float_div`, or `/` when either operand
is a float kind) computes in `f64` and rounds to the joined float kind. Unary negation (`TryNeg`)
range-checks signed integer kinds (refusing `MIN`), permits `0` on unsigned integer kinds and
refuses non-zero unsigned values, and negates floats and decimals. Running folds (`add_exact`,
`sub_exact`, and aggregate accumulators) widen inputs before accumulating so aggregate totals remain
`Int`, `Float`, or `Decimal`.

**Coercion (`TYPE`, function arguments, `RETURNS`) and casts (`<kind> x`, `type::<kind>(x)`).**

- Coercing or casting a number to an integer kind (`i8`…`u64`, `int`) succeeds if and only if the
  source number represents an exact integer within the target kind's domain; fractional values and
  out-of-range values are refused.
- Coercing or casting a number to `f16` or `f32` rounds to nearest-even and refuses a finite input
  that overflows to $\pm\infty$; non-finite float inputs preserve their sign and NaN/infinity class.
- Union coercion (`Kind::Either`) preserves a sized number whose exact kind is a member of the union
  before attempting conversions to other members, while `int`, `float`, and `decimal` try union
  members in declared order.
- `can_coerce_to_kind` agrees with `coerce_to_kind` and `can_cast` agrees with `cast` across all
  numeric kinds.
- `type::of` returns the value's exact kind name (`"i8"`, `"u8"`, …, `"f32"`). Built-in functions
  `type::i8`…`type::f32` and `type::is_i8`…`type::is_f32` cast to and test for each exact kind;
  `type::is_int` and `type::is_float` test for `Int` and `Float` specifically, while
  `type::is_number` is true for all twelve kinds.
- Single-value integer-preserving `math::*` functions (`abs`, `ceil`, `floor`, `round`, `sign`,
  `fixed`, `clamp`) preserve the input's kind (raising an overflow error on `math::abs(MIN)` for
  signed integer kinds); float-input `math::*` functions compute in `f64` and round back to the
  input's float width (`F16`, `F32`, or `Float`).
- `RecordIdKey::Number` holds `i64`: a sized integer record key that fits in `i64` converts to
  `RecordIdKey::Number`, and a `U64` above `i64::MAX` or a float key is refused. Array and object
  record-id keys preserve sized numbers.
- In `surrealdb-types`, `SurrealValue::into_value` for `i8`, `u8`, `i16`, `u16`, `i32`, `u32`, `u64`,
  `half::f16`, and `f32` widens to `Int` (or `Decimal` for `u64` above `i64::MAX`) and `Float` so
  schemaless writes through the Rust SDK produce the same stored kinds across embedded and remote
  engines, while `from_value` accepts any fitting integer (or any float for `f16`/`f32`/`f64`).

### 4. Persisted formats

**Values of existing kinds (`Int`, `Float`, `Decimal`) write the exact bytes they wrote previously
in every persisted format.**

- **`revision` encoding of `Number`:** `Int`, `Float`, and `Decimal` serialize at revision 1 with
  discriminants `0`, `1`, and `2`. The nine sized variants serialize at revision 2 with
  discriminants `3` through `11`. The revision deserializer accepts both revision 1 and revision 2,
  refusing discriminants $3\dots 11$ under revision 1. In-place `revision` readers decline
  revision-2 numbers (`revision != 1`) and fall back to full decode.
- **`revision` encoding of `Kind`:** `surrealdb_expr::Kind` is at revision 2 with the nine sized
  variants added at `#[revision(start = 2)]`.
- **Stored-value grammar (`surrealdb/expr/src/val/stored/`):** The nine sized kinds occupy scalar
  tags `0x29`–`0x31`:
  - `0x29` (`I8`): 1 byte, two's complement `i8`.
  - `0x2A` (`U8`): 1 byte, unsigned `u8`.
  - `0x2B` (`I16`): 1..=2 bytes, little-endian minimal two's complement `i16`.
  - `0x2C` (`U16`): 1..=2 bytes, little-endian minimal unsigned `u16`.
  - `0x2D` (`I32`): 1..=4 bytes, little-endian minimal two's complement `i32`.
  - `0x2E` (`U32`): 1..=4 bytes, little-endian minimal unsigned `u32`.
  - `0x2F` (`U64`): 1..=8 bytes, little-endian minimal unsigned `u64`.
  - `0x30` (`F16`): 2 bytes, little-endian IEEE 754 binary16 bits (`to_bits` / `from_bits`).
  - `0x31` (`F32`): 4 bytes, little-endian IEEE 754 binary32 bits (`to_bits` / `from_bits`).
  Unlike `INT` (which uses inline tags `0x80`–`0xFE` for `0..=126` of `Number::Int`), `I8`..`U64`
  encode their full domains under their own tags. Homogenous typed arrays (`I8_ARRAY`..`I64_ARRAY`,
  `F32_ARRAY`, `F64_ARRAY`) are used only for `Number::Int` and `Number::Float` elements so element
  widths are never conflated on decode.
- **Packed rows (`surrealdb/catalog/src/packed/`):** `PhysicalKind` includes `I8`, `I16`, `I32`,
  `I64`, `U8`, `U16`, `U32`, `U64`, `F16` (`#[stored(n = 10)]`, 2-byte fixed slot), `F32`, and
  `F64`. `leaf_column` maps each `SCHEMAFULL` scalar numeric kind to its matching `PhysicalKind`,
  and slot decoders return the column's own `Number` variant.
- **Index and record-id keys (`surrealdb/expr/src/val/key_encoding.rs`):** Index sort keys encode
  every `Number` by value through its widened lex-decimal representation (`Number::as_decimal_buf`
  via `DecimalLexEncoder`), so cross-width values share one index key and order by numeric value.
  Record-id component keys inside array and object record ids encode the lex-decimal value followed
  by the 1-byte `NumberKind` discriminant (`0`..`11`), ordering by numeric value first and kind
  second while preserving exact width on decode.

### 5. Wire formats

- **CBOR (`cbor` and `cbor.2`):**
  - Unnegotiated `cbor` (WebSocket subprotocol `cbor`, HTTP `application/cbor` without `v=2`) is
    frozen: encoders widen sized numbers to untagged CBOR integers or floats (or `TAG_STRING_DECIMAL`
    tag `10` for `Decimal` and `U64` values exceeding `i64::MAX`), and decoders refuse tags `180`–`188`.
  - Negotiated `cbor.2` (WebSocket subprotocol `cbor.2`, listed ahead of `cbor` in `PROTOCOLS`; HTTP
    `Accept: application/cbor; v=2` and `Content-Type: application/cbor; v=2`) wraps sized numbers in
    tags `180` (`TAG_I8`), `181` (`TAG_U8`), `182` (`TAG_I16`), `183` (`TAG_U16`), `184` (`TAG_I32`),
    `185` (`TAG_U32`), `186` (`TAG_U64`), `187` (`TAG_F16`), and `188` (`TAG_F32`). Decoders validate
    that each tagged integer or float fits the tag's domain and width.
  - On HTTP `/rpc`, `Content-Type: application/cbor` paired with `Accept: application/cbor; v=2` is
    permitted so a client's initial unnegotiated request can negotiate a `cbor.2` response; sending
    `Content-Type: application/cbor; v=2` with `Accept: application/cbor` is rejected with `415
    Unsupported Media Type`.
- **JSON, FlatBuffers, Protobuf, shaped, pgwire, mywire, GraphQL:**
  - JSON encodes sized numbers as standard JSON numbers (`U64` values as unsigned JSON integers).
  - FlatBuffers and Protobuf encode values through `n.wide()`; `Kind::to_fb` maps sized integer
    kinds (`I8`…`U64`) to `Number` and sized float kinds (`F16`, `F32`) to `Float` until
    `surrealdb-protocol` defines dedicated schema members.
  - Shaped frames (`surrealdb_types::shaped`) carry dedicated leaf kinds for `I8`…`U64`, `F16`
    (`0x17`), and `F32`, and use `cbor.2` for their envelope and dynamic CBOR slices. Because
    `cbor.2` and `shaped` produce identical bytes for all pre-existing types (`Int`, `Float`,
    `Decimal`, etc.), 3.x clients connecting to 4.0 continue to work when using existing types, and
    must upgrade to use the new sized number kinds; 4.0 clients connect to both 4.0 and 3.x
    servers.
  - `pgwire`, `mywire`, `sqlplan`, and GraphQL map each sized kind to its closest protocol column or
    scalar type and encode values through `n.wide()`.

## Consequences

- Every numeric value keeps its declared or literal width through expression evaluation, VM
  execution, stored documents, packed rows, record-id components, `ToSql` rendering, the Rust SDK's
  `SurrealValue` trait, and `cbor.2` RPC connections.
- Existing databases, frozen test fixtures, and 3.x peers see identical bytes for `int`, `float`,
  and `decimal` across `revision`, 4.0 records, index keys, and unnegotiated `cbor` / `flatbuffers`
  connections.
- Adding a new `Number` variant is a compile error at every width-sensitive match site, while
  value-only sites remain exhaustive over the three `Wide` variants.
