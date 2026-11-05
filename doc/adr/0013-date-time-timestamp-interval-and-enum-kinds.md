# ADR 0013: Date, time, timestamp, interval, and enum kinds

**Status:** Accepted · **Scope:** `surrealdb/types/src/` (`value/`, `kind/`, `shaped/`, `traits/`), `surrealdb/expr/src/` (`val/`, `expr/kind.rs`, `parser/`, `val/stored/`, `val/key_encoding.rs`), `surrealdb/parser-next/src/`, `surrealdb/catalog/src/` (`packed/`, `schema/`), `surrealdb/runtime/src/`, `surrealdb/core/src/` (`exec/`, `graphql/`, `rpc/format/`, `kvs/ds/sql_links.rs`), `surrealdb/sqlplan/src/`, `surrealdb/pgwire/src/`, `surrealdb/mywire/src/`

**Relates to:** ADR 0009 (the 4.0 record format), ADR 0011 (sized number kinds, the precedent for adding kinds across every surface).

## Open decisions

Every decision below is accepted as recommended, including the `surrealdb-protocol` release (D8) and the Rust SDK `chrono::NaiveDate` remap (D9). The kinds, SQL mappings, and enums land together in one change, built in dependency order. D14 stays a separate later item.

| # | Decision | Recommendation |
|---|---|---|
| D1 | Value layer: new `Value` variants or a tag on `Datetime`/`Duration` | Four new `Value` variants (`Date`, `Time`, `Timestamp`, `Interval`) appended after `Closure`. A tag on `Datetime` cannot carry months or days, cannot sort correctly, and makes round-trip typing depend on side data. |
| D2 | Literal syntax | Keyword-prefixed literals `pd'2026-10-27'`, `pt'12:30:00'`, `ts'2026-10-27T12:30:00'`, `ivl'1 year 2 days'`. `d'2026-10-27'` keeps meaning a midnight-UTC datetime. Alternatives in section 2. |
| D3 | Precision | Nanoseconds for `time` and `timestamp`, microseconds for the `interval` time part, truncated to microseconds at the PostgreSQL and MySQL wire. |
| D4 | Cross-kind comparison | Kinds stay distinct at the `Value` level (`Ord` is by rank, not by instant). The SQL front ends insert promotions (`date` to `timestamp` to `timestamptz`). SurrealQL operators do not promote implicitly. |
| D5 | Interval equality | PostgreSQL semantics: `Eq`, `Ord`, `Hash` use the normalised value (a month is 30 days, a day is 24 hours), so `1 mon = 30 days`. Display and storage keep the triple. |
| D6 | Coercion into a typed field | Lossless widening (`date` to `timestamp` to `datetime`) and exact narrowing (a `datetime` that is exactly a UTC midnight into `date`, any `datetime` into `timestamp`) coerce. Every other crossing needs an explicit cast. |
| D7 | Unnegotiated `cbor`, FlatBuffers, and protobuf | Degrade rather than refuse: `date` and `timestamp` encode as a datetime, `time` as a duration since midnight, `interval` as its ISO 8601 text. `cbor.2` carries dedicated tags. |
| D8 | `surrealdb-protocol` release for FlatBuffers and protobuf members | Needs the user's approval because it is a dependency bump. Until then the degraded form in D7 applies. |
| D9 | Rust SDK mapping of `chrono::NaiveDate` | Remap from `datetime` to `date` in 4.0 (a breaking change), and add `NaiveTime`, `NaiveDateTime`, and an interval type. |
| D10 | MySQL `TIME` | Maps to `interval`, because MySQL `TIME` is a signed duration up to 838:59:59, not a time of day. `time` kind backs PostgreSQL `TIME` only. |
| D11 | PostgreSQL `timetz` | Accepted on input (the offset is applied, the value is stored as `time`) and reported as `time`. No offset-carrying kind. |
| D12 | Enum surface | A catalog entity declared by `DEFINE ENUM`, referenced as `enum<name>`, plus an inline form `enum<'a', 'b'>` for column-scoped enums. Alternatives in section 8. |
| D13 | Enum ordering | Ordering by declaration order is applied by the planner through a rank function, not by `Value::cmp`. Indexes on enum columns serve equality and `IN` only in the first release. |
| D14 | Numeric modifiers | A separate later item. Store `numeric(p,s)`, `varchar(n)`, and `char(n)` as a numbered field on the field definition, never as an `ASSERT`. Precision above 28 digits is refused at DDL time. |
| D15 | `Kind` revision | Add the new kinds to `Kind` revision 2 (not released in any 3.x tag), and bump `Value` to revision 3 (revision 2 shipped in 3.3.4). |

## Context

SurrealQL has `datetime` (a UTC instant, `chrono::DateTime<Utc>`) and `duration` (an unsigned `std::time::Duration`). The PostgreSQL and MySQL front ends need `date`, `time of day`, `timestamp without time zone`, a calendar-aware `interval`, and named enum types, and none exists. Today every SQL front end collapses them:

- PostgreSQL `date`, `time`, `timestamp`, `timestamptz` and the `with/without time zone` spellings all lower to `Kind::Datetime` (`sqlplan/src/lower/ddl.rs`, `map_pgsql_type`). `interval` lowers to `Kind::Any`.
- MySQL `date`, `time`, `datetime`, `timestamp`, and `year` all lower to `Kind::Datetime`. `enum` and `set` lower to `Kind::String` with the member list discarded.
- `CAST(x AS date)` produces a datetime at 00:00:00Z through a `time::group(x, 'day')` rewrite. `type::of` still answers `datetime` and the wire still reports `timestamptz`. `CAST(x AS time)` and `CAST(x AS interval)` are no-ops.
- The reverse maps (`sqlplan/src/catalog/mod.rs` `kind_type_name`, `core/src/kvs/ds/sql_links.rs` `scalar_type`) report `Datetime` as `timestamp with time zone` and `Duration` as `interval`, so a column declared `DATE` is reported back as `timestamptz`.
- `pgwire` decodes `time`, `timetz` parameters into `Duration`, and `timestamp` and `timestamptz` both into `Datetime`. Binary `interval` input is refused when it has months, and negative intervals are refused. `age`, `make_interval`, and `to_date` do not exist.

`pg_enum` exists as an always-empty table. `CREATE TYPE` does not parse, and `DROP TYPE` parses and is refused at lowering. A column whose type is a user type lowers to `Kind::Any`.

ORM harnesses (Prisma, Drizzle, TypeORM) list type modifiers and enums among the top gaps, and `date`/`timestamp` columns round-tripping with the right wire type is a prerequisite for most drivers.

### Facts that shape the design

- There is one SurrealQL grammar. `surrealdb/parser-next` produces a flat tree and `surrealdb/expr/src/parser` lowers it into `Expr`, `Kind`, and literals (`expr/src/parser/mod.rs`, module documentation). `surrealdb/core/src/syn/mod.rs` is a wrapper over `surrealdb_expr::parser` with no lexer. New syntax lands once, in `parser-next` and its lowering.
- `Value` ordering, equality, hashing, the `storekey` discriminant byte, and the revision variant id all follow declaration order in `expr/src/val/mod.rs`. `variant_discriminant_order_matches_value_cmp` in `val/key_encoding.rs` pins it. A new variant must be appended, and cannot sit next to `Datetime` without renumbering every later variant.
- `Value` is at revision 2 in the 3.3.4 tag (`revision(1), revision(2, optimised)`), and `Kind` is at revision 1 in that tag. `Kind` revision 2 (the sized numbers) exists only on main.
- `Datetime` is `chrono::DateTime<Utc>` (about -262143 to +262142 years, nanosecond resolution, leap seconds representable). `Duration` is unsigned. Neither can express a negative or calendar quantity.
- `rust_decimal::Decimal` backs `decimal`: 96-bit mantissa, scale at most 28. Literals, casts, and CBOR tag 10 decode through `from_str_normalized`, which strips trailing zeros (`1.50dec` is stored as `1.5`).
- The 4.0 stored-value grammar has tags `0x32..=0x7F` free. `0x21..=0x25` are reserved for future typed arrays. A new tag is a new feature level, and a store that uses it refuses rollback to an earlier release (ADR 0009).
- The workspace denies `wildcard_enum_match_arm`, so every exhaustive `Kind` and `Value` match (about 80 files for `Kind`, about 190 for `Value`) fails to compile until it names the new variants. ADR 0011 touched the same sites.

## Decision

### 1. The four kinds

**`date`, `time`, `timestamp`, and `interval` become first-class kinds and values.** Both `surrealdb_types::Kind` and `surrealdb_expr::Kind` gain `Date`, `Time`, `Timestamp`, and `Interval`. Both `Value` enums gain `Date`, `Time`, `Timestamp`, and `Interval`, appended after `Closure` (discriminants 19 to 22 in the `storekey` and revision encodings).

| Kind text | `Value` variant | Payload | Domain |
|---|---|---|---|
| `date` | `Date` | `i32` days since 1970-01-01, proleptic Gregorian | `-262143-01-01` to `262142-12-31` (the `chrono::NaiveDate` range, matching `datetime`) |
| `time` | `Time` | `u64` nanoseconds since midnight | `00:00:00` to `24:00:00` inclusive, as PostgreSQL allows `24:00:00` |
| `timestamp` | `Timestamp` | `chrono::NaiveDateTime` as `(i64 seconds, u32 nanoseconds)` since 1970-01-01T00:00:00 wall time | the `datetime` range, leap seconds representable |
| `interval` | `Interval` | `(i32 months, i32 days, i64 microseconds)` | the full range of each component, as PostgreSQL |

`size_of::<Value>()` is unchanged: the largest payload is 16 bytes, below the 56-byte guard. Every new kind is a subtype of `Kind::Any` only. There is no common supertype for the three temporal kinds, and `Kind::Either` of them is the way to accept several.

**Why a new variant rather than a tag on `Datetime`.** A tag leaves `interval` unrepresentable (months and days), makes the sort order depend on a field that `Ord` and the index key would need to include, and means `type::of`, JSON, and the wire cannot tell the kinds apart without side data. A new variant costs the exhaustive-match updates ADR 0011 already paid for the sized numbers, and the compiler enumerates them.

**Precision.** `time` and `timestamp` keep nanoseconds, like `datetime`. `interval` keeps a microsecond time part, like PostgreSQL, which keeps the range of the time part at about 292,000 years rather than 292. Values that arrive with sub-microsecond digits in an interval are rejected at the SurrealQL surface and truncated at the SQL wire. PostgreSQL and MySQL both truncate to microseconds on output, as `pgwire` already does for `datetime`.

**Time zones.** `timestamp` and `date` carry no zone. The SQL session time zone is always UTC, so `timestamptz` is `datetime` and no zone database is added.

### 2. Literals, rendering, and the parser

**The recommended literal forms are keyword-prefixed strings.**

- `pd'2026-10-27'`
- `pt'12:30:00.123456789'`
- `ts'2026-10-27T12:30:00'` (also `ts'2026-10-27 12:30:00'`)
- `ivl'1 year 2 mons 3 days 04:05:06.5'` and `ivl'P1Y2M3DT4H5M6.5S'`

`parser-next` lexes a single `r`, `u`, `d`, `b`, `f`, or `s` immediately followed by a quote as a prefixed string (`lex.rs`). A longer identifier followed by a quote lexes today as an identifier then a string, which no production accepts, so the new forms change the meaning of no program that parses today. The golden corpus (`cargo make ci-parser`) confirms this in PR 2. Leaves are decoded at lowering time, as for `d'…'`.

`ToSql` renders a value as the same keyword-prefixed literal, so `INFO`, `EXPORT`, `DEFAULT` clauses, and result rendering round-trip. A `time` or `interval` literal is a constant, so it stays inert under `evaluation_is_inert` in `exec/planner/util/literals.rs`: the decoded value is pure and needs no clock or context.

`d'2026-10-27'` keeps its meaning: a `datetime` at midnight UTC.

Alternatives considered:

- **Kind-name prefixes** `date'…'`, `time'…'`, `timestamp'…'`, `interval'…'`. Each reads as a longer spelling of a kind name, and `date'…'` looks like an alias of `d'…'` while producing a different kind. Rejected in favour of `pd`, `pt`, `ts`, and `ivl`.
- **Single-letter prefixes.** `t'…'` and `i'…'` are free, but `date` has no free letter (`d` is `datetime`), so the set is inconsistent. Rejected.
- **Cast form `<date>'2026-10-27'`.** Valid today with no grammar change, and it remains the way to convert a string at runtime. It is an expression, not a literal, so it does not fold in the kind-literal and `DEFAULT` paths and renders back as a cast. Kept as the interim spelling for tests before PR 2 lands, not the canonical form.
- **Reuse `d'…'` and decide by body shape.** `d'2026-10-27'` is already valid and means a datetime. Changing it breaks stored `DEFAULT` clauses and exports. Rejected.
- **Duration-style interval literal (`1mo2d`).** `m` is minutes, `y` is a fixed 365 days, and `1mo` is a lex error today. A calendar unit in the duration grammar would make `1y` ambiguous. Rejected for this ADR.

**Kind keywords.** `date`, `time`, `timestamp`, `interval`, and `enum` are not keywords in `parser-next`, so they are matched by text in kind position (the same fallback as `i8` to `f64`) and collide with no field name or `time::` namespace call. They are not added to `RESERVED_KEYWORD`. `ast::kind` gains four codes and `SIMPLE_LIMIT` moves accordingly. `Kind`'s `ToSql` prints `date`, `time`, `timestamp`, `interval`.

**JSON and text forms.** Date `2026-10-27`; time `12:30:00.123456789`; timestamp `2026-10-27T12:30:00.5` (no zone designator); interval ISO 8601, `P1Y2M3DT4H5M6.5S`, with a negative component written as `P-1Y2M-3DT4H` (each component carries its own sign, as PostgreSQL's `iso_8601` style does). Text parsing accepts those and the PostgreSQL `postgres` style for intervals.

### 3. Equality, ordering, hashing, and arithmetic

**Cross-kind ordering follows rank.** The new variants sort after `Closure`, so `Value::cmp` between a `date` and a `datetime` compares by rank, not by instant. SurrealQL operators do not promote between kinds implicitly, which keeps `Ord`, `Eq`, `Hash`, and the `Value` index key consistent. The SQL front ends insert the PostgreSQL promotions (`date` to `timestamp` to `timestamptz`) as explicit casts during lowering, where the operand types are known. `hash_agrees_with_eq`, `index_format_spells_alike`, and `Value::equal` gain arms in PR 1.

**Within a kind, ordering is chronological.** `date` by days, `time` by nanoseconds, `timestamp` by `(seconds, nanoseconds)`.

**Interval uses PostgreSQL's normalised comparison.** `Eq`, `Ord`, and `Hash` compare the signed `i128` microsecond total `months * 30 * 86_400_000_000 + days * 86_400_000_000 + micros`, computed with every component widened to `i128` before any multiplication (the products overflow `i64` for large months or days), so `1 mon` equals `30 days` equals `720:00:00`. The triple is preserved for display and storage. `Value::identical` compares the triple. A `UNIQUE` index treats `1 mon` and `30 days` as one key.

**Arithmetic.** Existing results are unchanged (`datetime - datetime` is still `duration`, and `duration` stays unsigned).

| Expression | Result | Rule |
|---|---|---|
| `date + int`, `date - int` | `date` | whole days |
| `date - date` | `int` | days |
| `date + interval`, `date - interval` | `timestamp` | as PostgreSQL |
| `date + duration`, `date - duration` | `timestamp` | |
| `date + time`, `time + date` | `timestamp` | |
| `time ± interval`, `time ± duration` | `time` | wraps modulo 24 hours, months and days ignored |
| `time - time` | `interval` | time part only |
| `timestamp ± interval` | `timestamp` | months first (day clamped to the end of the month), then days, then the time part |
| `timestamp ± duration` | `timestamp` | |
| `timestamp - timestamp` | `interval` | days and time part, no months, as PostgreSQL's `-` |
| `datetime ± interval` | `datetime` | evaluated in UTC |
| `interval ± interval`, `-interval` | `interval` | component-wise, checked for overflow |
| `interval * number`, `interval / number` | `interval` | PostgreSQL rule: fractional months spill into days, fractional days into the time part |

Overflow of any component is an error (`ArithmeticOverflow`), never a wrap, except the intentional `time` wrap. Division by zero is an error.

### 4. Casts and coercion

**Casts (`<kind> x`, `type::<kind>(x)`).**

| From | To | Rule |
|---|---|---|
| `datetime` | `date` | the UTC calendar date (this fixes the `CAST(x AS date)` keeps-the-time bug) |
| `datetime` | `timestamp` | the UTC wall time |
| `datetime` | `time` | the UTC time of day |
| `timestamp` | `datetime` | the wall time read as UTC |
| `timestamp` | `date`, `time` | the date part, the time part |
| `date` | `datetime`, `timestamp` | midnight (UTC for `datetime`) |
| `time` | `interval` | the time part as microseconds |
| `time` | `duration` | nanoseconds since midnight |
| `duration` | `interval` | `(0, 0, micros)`, refusing sub-microsecond precision |
| `interval` | `duration` | `months * 30 days + days + micros` (PostgreSQL's epoch convention), refusing a negative total |
| `string` | each kind | the text forms in section 2, and for `date`, `time`, and `timestamp` also a full datetime string, which is parsed as a datetime and then cast |
| each kind | `string` | the text forms in section 2 |

`can_cast` agrees with `cast` for every pair. Casting `option<date>` and other unions follows the existing union rules.

**Coercion (`TYPE`, function arguments, `RETURNS`)** stays strict except for lossless crossings, so a SQL driver and an existing `datetime` column both keep working:

- Widening: `date` into `timestamp` or `datetime` (midnight), `timestamp` into `datetime` (as UTC).
- Exact narrowing: a `datetime` coerces to `date` only when it is exactly 00:00:00 UTC, and to `timestamp` always. A non-midnight `datetime` into a `date` field is refused with the cast to use in the message.
- Nothing else. A string never coerces to a temporal kind (it needs a cast), as `datetime` does today.

**`type::` functions.** `type::date`, `type::time`, `type::timestamp`, `type::interval`, and `type::is_date`, `type::is_time`, `type::is_timestamp`, `type::is_interval`. `type::of` answers the kind name.

### 5. Functions

Each function needs a descriptor, a `FALLIBILITY` row, a `builtin_paths` entry, a method-form registration, and (when clock-dependent) an effect row. There is no overload mechanism, so shared names declare an `Either` argument kind and use `ReturnRule::SameAsArg(0)` where the result follows the argument.

- **Extended to accept the new kinds.** `time::year`, `month`, `day`, `wday`, `yday`, `week`, `is_leap_year` accept `date`, `timestamp`, and `datetime`. `time::hour`, `minute`, `second`, `nano`, `micros`, `millis` accept `time`, `timestamp`, and `datetime`. `time::format`, `time::floor`, `time::round`, `time::group`, and the `time::set_*` functions accept `date`, `timestamp`, and `datetime` and return the argument's kind. `time::min` and `time::max` (and their aggregates) accept any one of the temporal kinds.
- **New in `time::`.** `time::make_date(year, month, day)`, `time::make_time(hour, minute, second)`, `time::make_timestamp(…)`, `time::age(a, ?b)` (calendar-aware `interval`, the PostgreSQL `age` algorithm), `time::extract(value, field)` (the PostgreSQL field set, including `century`, `decade`, `millennium`, `julian`, `isoyear`, `epoch` with fractional seconds, returning a number), and `time::trunc(value, unit)` (the generalised `group`, with `week`, `quarter`, `decade`, `century`, `millennium`).
- **New namespace `interval::`.** `interval::make(years, months, weeks, days, hours, mins, secs)`, the extractors `interval::years`, `months`, `days`, `hours`, `mins`, `secs`, `micros`, and `interval::justify_hours`, `justify_days`, `justify_interval`.
- **`duration::`** is unchanged.

The SQL front ends lower `to_char`, `age`, `date_trunc`, `extract`, `date_part`, `make_date`, `make_interval`, `make_timestamp`, `to_date`, `current_date`, and `current_time` onto these functions in PR 8. `current_date` returns `date`, `current_time` returns `time`, and the `time::group` rewrite and the `today()` helper are deleted.

### 6. Persisted formats

**Value, legacy `revision` encoding.** `Value` moves to revision 3 with `#[revision(start = 3, size = "varlen")]` on the four variants. Revision 2 shipped in 3.3.4, so the bump is required. The hand-written deserializer's variant ranges and `read_variant` match gain the four ids. Each payload is its own `#[revisioned(revision = 1)]` struct (`Date`, `Time`, `Timestamp`, `Interval`). `revision.lock` gets `Value:3` and four new lines, regenerated only from a plain clone, never inside a worktree.

**Kind, legacy `revision` encoding.** `Kind` revision 2 is not in any 3.x tag, so the four variants (and, in PR 13, `Enum`) join revision 2 with `#[revision(start = 2)]` and the next tags after the sized numbers (34 to 37). If any supported build is known to have written Kind revision 2 to a store that must survive, bump to revision 3 instead (D15). The `catalog-schema.lock` does not enumerate `Kind`, so only `revision.lock` changes for this; `PhysicalKind` changes only in PR 7.

**4.0 stored grammar.** Four scalar tags:

- `0x32` `DATE`: the day count as the `I32` payload (1 to 4 bytes, little-endian minimal two's complement).
- `0x33` `TIME`: nanoseconds as a little-endian minimal unsigned `u64` (1 to 8 bytes).
- `0x34` `TIMESTAMP`: the payload of `DATETIME` (`i64` nanoseconds since the epoch when it fits, otherwise `(i64 seconds, u32 nanoseconds)` in 12 bytes), so one canonical form exists per value.
- `0x35` `INTERVAL`: 16 bytes, `i32` months, `i32` days, `i64` microseconds, little-endian, the triple as written (not normalised).

The encoder, the strict decoder, the in-place `Kind::of` reader (`stored/read.rs`), `validate`, the schema `StoredField` impls, and `tests.rs` round-trips all gain the tags. The tags ship in the release that introduced the stored grammar, so no separate feature level exists: a pre-4.0 binary is already stopped by the stored-format ledger entry written with the first stored-grammar record (ADR 0009). A store still in the legacy `revision` record format writes a record holding a temporal value as `Value` revision 3 without that entry, so an older binary fails on that record; the sized number kinds have the same property.

**Index and record-id keys.** `Encode`/`BorrowDecode` for the three `storekey` formats (`()`, `IndexFormat`, `IndexIdFormat`) are written by hand for the four types, chronological from the first release (no stamp is needed because no index of these kinds exists yet):

- `Date`: sign-flipped big-endian `i32`.
- `Time`: big-endian `u64`.
- `Timestamp`: sign-flipped `i64` seconds then big-endian `u32` nanoseconds (the `Datetime` `IndexFormat` layout, in every format including record-id keys).
- `Interval`: in the `()` and `IndexFormat` encodings, the sign-flipped `i128` normalised total (16 bytes) only, so a `UNIQUE` index enforces uniqueness on the normalised total with a point lookup and `1 mon` and `30 days` are one key. A key cannot recover the exact triple, so the planner never serves an interval column from an index key alone and reads the record. In the `IndexIdFormat` encoding (record-id components) the 16-byte prefix is followed by the exact triple (4, 4, and 8 bytes, each sign-flipped), so a record id stays addressable by its written value, following the `Number` kind-byte precedent.

`variant_discriminant_order_matches_value_cmp` gains four samples. The planner serves an indexed column of any of the four kinds for equality, `IN`, ranges, and `ORDER BY`: the key order of each is the order `Value::cmp` gives, and an interval column's equal-by-total values share one key, so an equality is exact. An index key does not hold an interval's exact triple, so no plan projects an interval from the key alone. A literal of another kind than the column compares by kind rank, and the key opens with the kind in that same order, so the scan returns the rows a table scan returns. A date, time, timestamp, or interval nested in a record-id key is not an index operand, because the record-id spelling of an interval carries the triple. The key-structure rules of aggregate views and record-id ranges (`exec/index/key_range.rs`) keep declining the four kinds.

**Packed rows.** PR 1 stores the four kinds in the generic `PhysicalKind::Value` column, which is correct and needs no catalog change. PR 7 adds `PhysicalKind::Date` (4 bytes), `Time` (8), `Timestamp` (the `Datetime` slot layout), and `Interval` (16), numbered 35 and up, with `catalog-schema.lock` and golden updates.

**Shaped frames.** Leaf codes `0x1a` to `0x1d` (free) for the four kinds in PR 1, or `Dynamic` until then.

### 7. Wire formats and SDKs

**CBOR.** `cbor.2` carries dedicated tags: `100` for `date` (RFC 8943, integer days since the epoch, readable by standard libraries), `189` for `time` (unsigned nanoseconds), `190` for `timestamp` (`[seconds, nanoseconds]`, the shape of tag 12), and `191` for `interval` (`[months, days, microseconds]`). Unnegotiated `cbor` refuses decoding these tags and degrades encoding (D7): `date` and `timestamp` as tag 12 datetimes, `time` as a tag 14 duration, `interval` as ISO 8601 text. No JS SDK document specifies tag allocation, so the tag table is the contract and is added to `doc/design/` with PR 1.

**JSON** uses the text forms in section 2. **FlatBuffers and protobuf** live in the external `surrealdb-protocol` crate and have no member for the new kinds. `Kind::to_fb` and `Value::to_fb` apply the D7 degradation until a protocol release adds them (D8). The decode side refuses an unknown member with an error, never `NONE`.

**GraphQL.** Scalars `date`, `time`, `timestamp`, and `interval`, parsed and serialised as the text forms, with `gt`/`gte`/`lt`/`lte` filters for the first three. The GQL dialect lexes `DATE`, `TIME`, and `TIMESTAMP` literals but its parser rejects them today, so lowering them to the new constructors is a separate change to that dialect.

**Rust SDK.** `SurrealValue` is implemented for `chrono::NaiveDate` (`kind!(date)`, replacing today's lossy `datetime` mapping), `chrono::NaiveTime` (which cannot represent `24:00:00`, so reading that value into a `NaiveTime` is a conversion error that names the limit, while the `time` kind and the SDK's own `Time` type carry it), and `chrono::NaiveDateTime`, and for a new public `Interval` type. The derive macro learns `date`, `time`, `timestamp`, and `interval`.

### 8. Enums

**An enum is a string value constrained to an ordered, named set of labels.** No new `Value` variant exists: the stored value is a `String`, and membership is validated on write. Only `Kind` gains `Enum(EnumRef)`, where `EnumRef` is `Named(name)` or `Inline(labels)`.

**SurrealQL surface (D12).** Options:

1. **`DEFINE ENUM` (recommended).** `DEFINE ENUM mood VALUES 'sad', 'ok', 'happy'`, `ALTER ENUM mood ADD VALUE 'ecstatic' AFTER 'happy'`, `ALTER ENUM mood RENAME VALUE 'ok' TO 'fine'`, `REMOVE ENUM mood`, with `INFO FOR DB` and `EXPORT` support, used as `TYPE enum<mood>`. It matches PostgreSQL's `CREATE TYPE … AS ENUM`, carries an ordering, and fits the existing `DEFINE`/`ALTER`/`REMOVE` family.
2. **A named alias for any kind** (`DEFINE TYPE mood AS 'sad' | 'ok' | 'happy'`). General, but an `Either` of literals has no declaration order, no `ADD VALUE ... AFTER`, and no rename. It cannot back `pg_enum`.
3. **No named entity** (extend `Kind::Literal` with a per-field enum). Covers MySQL's column-scoped `ENUM`, but not a PostgreSQL type shared across tables.

The ADR recommends option 1 plus the `Inline` form of option 3 for column-scoped MySQL enums. Existing `'a' | 'b'` literal unions are unchanged and remain the way to write an unordered, unnamed set.

**Catalog entity.** `EnumDefinition { name, id, labels: Vec<(label, sort key)> }`, a database-scoped entity following the `DEFINE BUCKET` template (a revisioned `Stored` struct and `catalog-schema.lock` lines, a key in `datastore/src/key/schema.rs`, `Transaction` provider methods and cache entry, statements, logical-plan nodes, `INFO FOR DB`, `EXPORT`, frozen fixtures, rollback handling). `id` is a database-scoped allocated `u32`, stable across renames. Sort keys are `u64`s allocated with gaps, so `ADD VALUE … BEFORE`/`AFTER` takes a midpoint without rewriting rows. `REMOVE ENUM` fails while any field references it, and removing a single label fails while any stored row uses it. The permission model follows `DEFINE PARAM`.

**Ordering (D13).** `Value::cmp` is context-free, so a label compares lexically there. Declaration-order comparison is applied where the kind is known: the planner rewrites `ORDER BY`, `<`, `>`, `<=`, `>=`, and `BETWEEN` over an `enum<…>` column or expression to compare `enum::rank(name, x)` (a new function reading the cached catalog entry), and rewrites the aggregates `min` and `max` to `enum::min(name, x)` and `enum::max(name, x)`, which compare by rank and return the label, not the rank. Indexes on enum columns key by label, so the planner uses them only for `=` and `IN`, and sorts for ordering. A later change can key an enum index by sort key if profiling asks for it.

**Coercion.** A string coerces into `enum<…>` only when it is a member. A member coerces into `string`. `enum<a>` into `enum<b>` needs a cast through `string`. Inline enums compare by their declared order.

**MySQL.** `ENUM('a','b')` lowers to `enum<'a', 'b'>`, sorted by member index. `SET('a','b')` lowers to `set<enum<'a', 'b'>>` with members normalised to declaration order on write, within MySQL's 64-member limit. The wire emits type codes `0xf7` (`ENUM`) and `0xf8` (`SET`) with `ENUM_FLAG`/`SET_FLAG`.

**PostgreSQL.** `CREATE TYPE … AS ENUM`, `ALTER TYPE … ADD VALUE [IF NOT EXISTS] [BEFORE|AFTER x]`, `ALTER TYPE … RENAME VALUE`, and `DROP TYPE` (new AST variants in `surrealdb/pgsql`, lowering to the statements above). `pg_type` gains `typtype = 'e'` rows and `pg_enum` gains one row per label, with `enumsortorder` derived from the sort key and an `oid` derived from `(type id, sort key)`. A type's OID is 2097152 (2^21) plus its id, clear of the extension precedent (the vector type at 16400) and of relation OIDs, and its array type's OID is a further 2^20 above. Enum values are sent in text format.

### 9. SQL front-end mappings

| PostgreSQL | Kind | OID | Text | Binary |
|---|---|---|---|---|
| `date` | `date` | 1082 | `2026-10-27` (BC suffix for negative years) | `i32` days since 2000-01-01 |
| `time`, `time without time zone` | `time` | 1083 | `12:30:00.123456` | `i64` microseconds |
| `timetz`, `time with time zone` | `time` (D11) | 1266 on input, 1083 on output | | |
| `timestamp`, `timestamp without time zone` | `timestamp` | 1114 | `2026-10-27 12:30:00.123456` | `i64` microseconds since 2000-01-01 |
| `timestamptz` | `datetime` | 1184 | `… +00` | as `timestamp` |
| `interval` | `interval` | 1186 | `1 year 2 mons 3 days 04:05:06.5` | `i64` microseconds, `i32` days, `i32` months |
| user enum | `enum<name>` | 2097152 + id | the label | the label bytes |

| MySQL | Kind | Wire code |
|---|---|---|
| `DATE` | `date` | `0x0a` |
| `TIME` | `interval` (D10) | `0x0b`, text clamped to `±838:59:59` |
| `DATETIME` | `timestamp` | `0x0c` |
| `TIMESTAMP` | `datetime` | `0x07` |
| `YEAR` | `u16` | `0x0d` |
| `ENUM`, `SET` | `enum<…>`, `set<enum<…>>` | `0xf7`, `0xf8` |

`infinity` and `-infinity` for dates and timestamps stay unsupported (the existing `datetime` strings keep working). The mappings change in `map_pgsql_type`, `map_mysql_type`, `cast_kind`, `kind_type_name`, `scalar_type`, `SqlType::from_name`, `PgType::from_sql_type`, `pg_type`, `format_type`'s `PG_TYPES`, and `base_type`/`promote`. The two `Kind`-to-SQL maps (`sqlplan/src/catalog/mod.rs` and `core/src/kvs/ds/sql_links.rs`) are exhaustive on `Kind`, so the compiler lists the sites. `pgwire` stops decoding `time`/`timetz` parameters into `Duration` and `timestamp` parameters into `Datetime`, decodes them into the new kinds, and accepts a `datetime` value for a `timestamp` column through the coercion in section 4 (D6).

### 10. Migration

- A stored `datetime` or `duration` value stays valid in every field that declares `datetime`, `duration`, `any`, or an `option` of those. Nothing is rewritten on upgrade.
- `DEFINE FIELD f ON t TYPE date` over existing `datetime` rows does not rewrite the rows. Reads return the stored `datetime`. A write that touches the field applies section 4: a stored value at exactly midnight UTC coerces, and any other value is refused with the cast to use. `UPDATE t SET f = <date> f` is the documented backfill.
- SQL `ALTER TABLE … ALTER COLUMN … TYPE date` lowers to the field change plus that backfill `UPDATE`, as PostgreSQL's implicit `USING` cast does.
- Tables created through SQL DDL before this change hold columns declared `DATE` or `TIMESTAMP` as `TYPE datetime`. They keep working: parameters of those OIDs decode into the new kinds and coerce into the `datetime` column (D6). Such a column is reported as `timestamptz` until redefined, and the documentation says so.
- Range predicates over a column holding values of two kinds, as during a partial backfill after `DEFINE FIELD ... TYPE timestamp` over `datetime` rows, compare the two kinds by rank, so a range is exact only once the backfill has finished. Equality over the rows already converted is exact throughout.
- `DEFINE FIELD OVERWRITE` narrowing differs by origin. On a database at compatibility level 4, narrowing a field that has rows needs `MIGRATE EXISTING ROWS`. On a store created by 3.x it keeps the rows as written and the backfill `UPDATE` converts them.
- A 4.0 store that has written any new tag or variant refuses rollback to an earlier release through the feature-level gate.

## Numeric modifiers and length limits (a separate later item)

This ADR assesses it and does not decide it for PR 1 to 11.

- **Today.** `numeric(p,s)`, `varchar(n)`, `char(n)`, and `timestamp(n)` are parsed, then dropped (`TypeName.modifiers`, `map_pgsql_type`). `atttypmod` is -1, `character_maximum_length` is NULL, and `numeric_precision` and `numeric_scale` are NULL for `numeric`. Nothing enforces them.
- **What backs `decimal`.** `rust_decimal::Decimal`, 96-bit mantissa, scale 0 to 28. PostgreSQL allows 131,072 digits before the point and 16,383 after, so values or declared precisions beyond about 28 digits cannot round-trip. `numeric(p,s)` with `p <= 28` can. The value itself keeps its scale, but SurrealQL literals, casts, and CBOR tag 10 decode through `from_str_normalized` and lose trailing zeros, and equality and hashing ignore scale. A declared scale on the field, applied on write with `Decimal::rescale(s)`, restores `1.50` for `numeric(10,2)` and gives PostgreSQL's rounding behaviour.
- **Where to store it (D14).** The field-level `ASSERT` slot is single-valued: `NOT NULL` and foreign-key lowering both write it, and `ALTER COLUMN … TYPE` rewrites it, so a length check stored there is clobbered, and `information_schema` cannot recover `(n)` from an opaque expression. The recommendation is a numbered, ignorable field on the stored field definition carrying `{ precision, scale }` or `{ max_length, padded }`, enforced by the write path, reported through `atttypmod` and `information_schema`. That is a `catalog-schema.lock` change plus the write-path enforcement, separate from this ADR's kinds.
- **Limits to document.** `numeric` with a precision above 28 is refused at DDL time. `char(n)` pads on output, not in storage. `timestamp(n)` and `time(n)` round the fractional digits to `n`.

## Upgrade-test plan

No released binary can write `date`, `time`, `timestamp`, `interval`, or an enum, so no upgrade pair can run for them yet. The repository rule is explicit: until a release can write a construct, no axis reaches it, `introduced` gating produces zero runs (or a load error in a chain), and the construct belongs in ordinary language tests.

- **PR 1 to 4 (language tests).** Ordinary language tests for every kind, cast, coercion, arithmetic row, index, and `EXPLAIN` in this ADR.
- **Pairs that can run now.** Stock 3.x writes `datetime` and `duration` fields (including `option<…>` and indexed ones). The current build reads them, and the retype and cast happen in the test body on the new binary, with assertions on query results and a `_current` file asserting the index plan. This proves the migration behaviour in section 10.
- **Byte fixtures.** PR 1 adds frozen fixtures to `core/src/kvs/compat/` (`stored_record.rs` for the new stored tags, a legacy `revision` fixture for `Value` revision 3 and the `Kind` variants), each decoding identically through the VM.
- **Gated pairs.** When a release (a beta counts) first writes the new kinds, add pairs prefixed and gated with that release: an indexed `date`, `timestamp`, `time`, and `interval` field (each exercising the index key layout, so an unreadable index surfaces as a plan difference), a field of each kind in a record id, and an enum field with a renamed and an inserted label. Add the release to all three version lists in `Makefile.ci.toml`, verifying the hop first.
- **Repairs.** None expected. The new index layouts are born chronological, so no `REBUILD INDEX` is needed, and a body must never contain one.

## Revision and lock impact

- `revision.lock`: `Value:3` and four new payload lines in PR 1, `Kind:2` with a new hash (and `EnumRef` lines in PR 13). Regenerated from a plain clone.
- `catalog-schema.lock`: unchanged by PR 1 to 6. PR 7 adds `PhysicalKind` codes 35 and up, PR 12 adds the typmod field, and PR 13 adds `EnumDefinition`.
- Feature level: none new for the stored tags (see section 6). The enum catalog entity is a new stored type under the same ledger.
- `StoredFieldDefinition`, `StoredFunctionDefinition`, and `StoredIndexDefinition` embed `Kind`, so their `revision.lock` lines change with it.

## WebAssembly size impact

There is no recorded per-kind cost. ADR 0011 added nine kinds and recorded none. No new dependency is introduced: `chrono::NaiveDate`, `NaiveTime`, and `NaiveDateTime` are already linked through `Datetime`, so the cost is code for the new match arms, the four parsers and formatters, and the new functions, with the largest monomorphic additions in `exec/function` and the CBOR and `storekey` codecs. The lean budget is 15,000,000 bytes on nightly (`scripts/wasm-size-budget.sh`). Every PR measures before and after with `scripts/wasm-size-budget.sh --toolchain nightly --variant lean --names`, and PR 1 reports the total and the largest per-function deltas. The SQL front ends and enums are not in the lean variant's feature set.

## PR breakdown

Each PR carries its language tests. Exhaustive matches force a kind or value to land with its arms in every crate, so PR 1 is large but mechanical.

1. **Kinds and values.** The four kinds and values in `types` and `expr`, `Value` revision 3, `Kind` revision 2 variants, `Ord`/`Eq`/`Hash`, `storekey` formats, stored tags and feature level, shaped and generic packed fallback, JSON and `cbor.2` tags, degraded unnegotiated `cbor`, flatbuffers and protobuf degradation, `convert_public`, the exhaustive arms in `exec`, `catalog`, `sqon`, `runtime`, `pgwire`, `mywire`, GraphQL, `sqlplan` (temporary: new kinds map to the closest existing SQL type), compat fixtures, `variant_discriminant_order_matches_value_cmp`, `revision.lock`. No syntax yet, so values only arise from Rust.
2. **Parsing and rendering.** Kind keywords, keyword-prefixed literals, `ToSql`, text parsing and formatting, golden bless, fuzz dictionary, language tests for syntax.
3. **Casts and coercion.** Section 4, including `type::` functions and the `CAST(x AS date)` fix at the engine level.
4. **Arithmetic and comparison.** Section 3.
5. **Functions.** The `time::` extensions and additions, and the `interval::` namespace.
6. **SDK and GraphQL.** `SurrealValue` impls, derive, GraphQL scalars and filters.
7. **Packed rows.** The four `PhysicalKind` slots (optional, performance only).
8. **SQL mapping.** `sqlplan` DDL, `CAST`, function lowering (`to_char`, `age`, `date_trunc`, `extract`, `make_*`, `to_date`, `current_date`, `current_time`), descriptors, `information_schema`, `pg_type`; the rewrites this replaces are deleted.
9. **PostgreSQL wire.** `typing.rs` and `encode.rs` for the new kinds, intervals with months, days, and negatives.
10. **MySQL wire.** `typing.rs` and the `DATE`, `TIME`, `DATETIME`, `TIMESTAMP`, `YEAR` mappings.
11. **Upgrade pairs for the migration path** (3.x writes `datetime`, the new binary retypes), and the documentation of the tag table.
12. **Numeric modifiers and length limits** (separate, after review of the assessment above).
13. **Enum catalog entity.** `EnumDefinition`, `DEFINE`/`ALTER`/`REMOVE ENUM`, `INFO`, `EXPORT`, `Kind::Enum`, coercion, the planner rewrite and `enum::rank`.
14. **SQL enums.** `CREATE TYPE`, `ALTER TYPE`, `DROP TYPE`, `pg_type` and `pg_enum`, MySQL `ENUM` and `SET`, wire types and flags.
15. **Gated upgrade pairs** for each new construct, in the release that first writes it.

## Compatibility

The new kinds and the new SQL mappings are always available. No capability flag, database option, or session setting gates them.

- Stored values, indexes, and records of existing kinds keep their exact bytes. The new kinds add variants, tags, and discriminants after the current ones, and nothing is renumbered.
- The new tags ship in the release that introduced the stored grammar, so a store in the stored format is already refused by every earlier release. A store still in the legacy `revision` record format can write a temporal value that an earlier release cannot decode, as it can for the sized number kinds.
- Unnegotiated `cbor`, FlatBuffers, and protobuf clients receive the degraded forms in section 7.
- The SQL mappings in section 9 apply to every database. Tables created before the change keep the `datetime`, `duration`, `any`, and `string` field types they were created with, and tables created after it get the new kinds. Existing SQL clients see the following differences, all intended: `CAST(x AS date)` and `current_date` return a `date`, an `interval` column is typed, MySQL `TIME` is an `interval`, MySQL `YEAR` is a `u16`, and MySQL `ENUM` and `SET` enforce their members.
- The coercions in section 4 keep writes from SQL drivers working against existing `datetime` columns.
- The Rust SDK writes `date` for `chrono::NaiveDate` and accepts a `date` or a `datetime` when reading, so existing callers keep working against existing `datetime` fields.

## Consequences

- `date`, `time`, `timestamp`, and `interval` round-trip through SurrealQL, storage, indexes, the SDKs, and both SQL wires with their own types.
- Existing `datetime` and `duration` fields, values, indexes, and records keep their exact bytes and behaviour. The only visible change for existing data is that a field retyped to `date` or `timestamp` needs the backfill in section 10.
- Cross-kind comparison in SurrealQL stays explicit, which keeps ordering, hashing, and index keys consistent. SQL front ends carry the promotion rules.
- Adding the kinds is a compile error at every exhaustive match, as for the sized numbers.
- Enum ordering is applied by the planner, so an index cannot serve an enum range scan in the first release.
- A store that has written any of the new tags refuses rollback to an earlier release.
