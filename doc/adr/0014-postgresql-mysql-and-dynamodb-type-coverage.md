# ADR 0014: PostgreSQL, MySQL, and DynamoDB type coverage

**Status:** Accepted · **Scope:** `surrealdb/sqlplan/src/` (`lower/ddl.rs`, `lower/expr.rs`, `catalog/`, `descriptor/`), `surrealdb/pgwire/src/`, `surrealdb/mywire/src/`, `surrealdb/catalog/src/` (field definition), `surrealdb/expr/src/` (`val/`, `expr/kind.rs`) for D2 only, `surrealdb/core/src/kvs/ds/sql_links.rs`

**Relates to:** ADR 0011 (sized number kinds, the checklist for adding a value kind), ADR 0013 (date, time, timestamp, interval, and enum kinds; its "numeric modifiers" item, D14, is folded into D1 here).

## Open decisions

Every built-in type of PostgreSQL 17 and MySQL 8.4, and every DynamoDB attribute type, is listed in the coverage tables below with what it maps to now and what it maps to under this ADR. Only one of them needs a new value kind (D2). The rest need a mapping change, a wire codec, or the declared SQL type of D1.

| # | Decision | Recommendation |
|---|---|---|
| D1 | Where a column's SQL type lives when the kind alone cannot express it | A numbered, ignorable `sql_type` field on the stored field definition: the dialect, the type name, and its modifiers (`varchar(20)`, `numeric(10,2)`, `bit(8)`, `inet`, `timestamp(3)`, `point`). The write path enforces it (length, padding, precision, scale, fractional seconds, text syntax) and the wire layers, `atttypmod`, and `information_schema` report it. This is ADR 0013 D14, widened from modifiers to whole types. Never an `ASSERT`. In SurrealQL it is the clause `SQL TYPE <dialect> '<text>'` of `DEFINE FIELD`, with the dialect `POSTGRESQL`, `MYSQL`, or `DYNAMODB` (for example `DEFINE FIELD n ON t TYPE decimal SQL TYPE POSTGRESQL 'numeric(10,2)'`), so `INFO`, export, and import carry it. A column read through the other dialect's wire is reported by the nearest type of that dialect. |
| D2 | Numbers beyond `decimal` (28 digits): DynamoDB `N` (38 digits, exponent -130 to +125), MySQL `DECIMAL(65,30)`, PostgreSQL `numeric` above precision 28 | A new arbitrary-precision number `bigdecimal`: a `Number::BigDecimal` variant and `Kind::BigDecimal`, as ADR 0011's sized numbers are, using the packed code 30 already reserved for it. The type is our own (`BigDecimal`: a `num_bigint::BigInt` coefficient and an `i64` scale), so its rounding, scale, equality, and text form are defined here, not by a library. `num-bigint` is already in the build through `jsonwebtoken`, so this adds a direct dependency edge and no new crate. Equality and hashing normalise trailing zeros, so `1.50` equals `1.5`. Division rounds half away from zero, as PostgreSQL and MySQL do, to `max(20, scale of the operands) + 4` fractional digits; the other `math::*` functions convert to `decimal` and refuse a value outside its range. |
| D3 | `i128`, `u128`, and `bigint` kinds | None now. No type in the three systems needs them: PostgreSQL and MySQL integers fit `i64` and `u64`, and `numeric(p, 0)` beyond that is a `bigdecimal` with scale 0. Codes 29, 31, and 32 stay reserved. |
| D4 | Text-shaped types (`inet`, `cidr`, `macaddr`, `macaddr8`, `bit`, `varbit`, `xml`, `tsvector`, `tsquery`, `jsonpath`, `citext`, `name`, `"char"`, `tid`, `pg_snapshot`) | `string` with the D1 type. The write path validates and canonicalises the text (for example `inet` drops a redundant `/32`), and the wire reports the type's OID. `inet` and `cidr` order by address, not text, so the SQL planner sorts and compares them through a rank function, as ADR 0013 D13 does for enums. SurrealQL keeps string order. |
| D5 | Number-shaped types (`money`, `oid` and the `reg*` aliases, `xid`, `cid`, `xid8`, `pg_lsn`, MySQL `BIT(n)`, `YEAR`, `BIGINT UNSIGNED`, `SERIAL`) | An existing number kind with the D1 type: `money` is `decimal` with scale 2; `oid`, `reg*`, `xid`, `cid` are `u32`; `xid8` and `pg_lsn` are `u64` (`pg_lsn` prints as `X/Y`); MySQL `BIT(n)` is `u64`; `BIGINT UNSIGNED` and `SERIAL` are `u64`; `YEAR` is `u16` sent as wire type `0x0d`. |
| D6 | Geometric and spatial types | PostgreSQL `point`, `lseg`, open `path`, closed `path`, `polygon`, and `box` map to `geometry<point>`, `geometry<line>`, `geometry<line>`, `geometry<polygon>`, `geometry<polygon>`, and `geometry<polygon>`, with PostgreSQL's text and binary forms on the wire. `line` (`{A,B,C}`) and `circle` have no GeoJSON form and are `object` values of a fixed shape with the D1 type. MySQL's spatial types map to the matching `geometry<…>` kinds and travel as WKB with the 4-byte SRID prefix; the column's `SRID` attribute is part of the D1 type. |
| D7 | Range and multirange types | The six built-in ranges map to `range` with the element type in D1, so `int4range` refuses a range of strings. An empty range is a half-open range with equal bounds and prints as `empty`. A multirange is `array<range>`, kept sorted and merged on write. |
| D8 | `json` and `jsonb` | `any` with the D1 type, restricted on write to values JSON can carry. They map to `object` today, which refuses `'[1,2]'::json` and `'5'::jsonb`. `jsonb` keeps the last of duplicate keys; `json` keeps its text. |
| D9 | Domains and composite types | Catalog entities declared by SQL only, as ADR 0013 enums are, with OIDs in their own range. A domain column has its base kind, its `NOT NULL`, and its `CHECK` joined into the field's `ASSERT` with the column's own. A composite column is an `object` of the declared field kinds. Built last; nothing else waits on it. |
| D10 | `infinity` and `-infinity` for `date`, `timestamp`, and `timestamptz` | The greatest and least value each kind holds, printed as `infinity` and `-infinity` on the PostgreSQL wire and parsed back from them. They sort after and before every other value, as in PostgreSQL. ADR 0013 left these unsupported. |
| D11 | `timetz` with its offset | Keep ADR 0013 D11 (the offset is applied and dropped). PostgreSQL's documentation discourages the type and no ORM in the harnesses writes it; a kind for it would be the only value kind with a zone. |
| D12 | Multi-dimensional arrays | `int[][]` maps to `array<array<int>>`. A rectangular array of arrays travels as a multi-dimensional PostgreSQL array rather than as `jsonb`. Lower bounds other than 1 are refused. |

## Context

The PostgreSQL and MySQL front ends map a column type to a SurrealQL kind in `map_pgsql_type` and `map_mysql_type` (`sqlplan/src/lower/ddl.rs`). A type either function does not name lowers to `any`, so the column accepts every value, the wire reports it by the value's type, and nothing round-trips the type the client declared. After ADR 0013 the named types are the integers, floats, `numeric`, text, `bool`, `uuid`, `bytea`, `json`, the date and time types, `interval`, enums, and pgvector's `vector`. Type modifiers are parsed and dropped.

The DynamoDB data model has ten attribute types. Nine map onto existing values (`object`, `array`, `set`, `string`, `bytes`, `bool`, `null`). `N` does not: it carries 38 significant digits, and `decimal` (`rust_decimal`, a 96-bit mantissa) holds 28.

### Facts that shape the design

- A wire type comes from the declared column kind first (`declared_column_type` in `pgwire/src/typing.rs`, `kind_type_name` in `sqlplan/src/catalog/mod.rs`) and from the value otherwise. A type stored as a plain `string` is reported as `text` unless the column says more, which is what D1 provides.
- The field-level `ASSERT` holds one expression that `NOT NULL`, foreign keys, and `ALTER COLUMN … TYPE` each rewrite (ADR 0013, "numeric modifiers"), so it cannot carry a type.
- A new value kind costs every exhaustive `Value` and `Kind` match (about 190 and 80 files), a stored-grammar tag, CBOR tags, SDK types, and a feature level that refuses rollback (ADR 0009, ADR 0011). D2 pays this once; nothing else in this ADR does.
- `rust_decimal` is the only decimal type in the build. `num-bigint` is already compiled into `surrealdb-expr` through `jsonwebtoken` and `simple_asn1`.
- `Kind::Range` carries no element kind, and `Kind::Geometry` carries no SRID.

## Coverage: PostgreSQL

"Today" is this branch after ADR 0013. "Mapping" means a change to `map_pgsql_type` and the reverse maps only.

| Type | Today | Under this ADR | Needs |
|---|---|---|---|
| `smallint`, `integer`, `bigint`, `smallserial`, `serial`, `bigserial` | `i16`, `i32`, `int` | unchanged | |
| `real`, `double precision`, `float(p)` | `f32`, `float` | unchanged | |
| `numeric`, `numeric(p,s)`, `decimal` | `decimal`, modifiers dropped | `decimal` for `p <= 28`, `bigdecimal` above or unconstrained, scale applied on write | D1, D2 |
| `money` | `any` | `decimal`, scale 2 | D5 |
| `text`, `varchar(n)`, `char(n)`, `bpchar` | `string`, length dropped | `string`, length enforced, `char` padded on output | D1 |
| `name`, `"char"`, `citext` | `any` | `string` | D4 |
| `bytea` | `bytes` | unchanged | |
| `bool` | `bool` | unchanged | |
| `date`, `time`, `timestamp`, `timestamptz`, `interval` | ADR 0013 kinds | plus `(p)` fractional rounding, interval fields, and `infinity` | D1, D10 |
| `timetz` | `time`, offset dropped | unchanged | D11 |
| `uuid` | `uuid` | unchanged | |
| `json`, `jsonb` | `object` | `any`, JSON values only | D8 |
| `jsonpath` | `any` | `string` | D4 |
| `xml` | `any` | `string`, well-formed | D4 |
| `bit(n)`, `bit varying(n)`, `varbit` | `any` | `string` of `0` and `1`, length enforced | D4 |
| `inet`, `cidr`, `macaddr`, `macaddr8` | `any` | `string`, canonical, address order in SQL | D4 |
| `point`, `lseg`, `path`, `polygon`, `box` | `any` | `geometry<…>` | D6 |
| `line`, `circle` | `any` | fixed-shape `object` | D6 |
| `int4range`, `int8range`, `numrange`, `tsrange`, `tstzrange`, `daterange` | `any` | `range`, element in D1 | D7 |
| the six multirange types | `any` | `array<range>` | D7 |
| `tsvector`, `tsquery` | `any` | `string`, canonical | D4 |
| `oid`, `regclass`, `regproc`, `regprocedure`, `regoper`, `regoperator`, `regtype`, `regnamespace`, `regrole`, `regconfig`, `regdictionary`, `regcollation`, `xid`, `cid` | `any` | `u32` | D5 |
| `xid8`, `pg_lsn` | `any` | `u64` | D5 |
| `tid`, `pg_snapshot`, `txid_snapshot` | `any` | `string` | D4 |
| `T[]` | `array<T>` | unchanged | |
| `T[][]` | `array<T>` | `array<array<T>>` | D12 |
| enums | `enum<name>` | unchanged | |
| domains, composites | refused | catalog entities | D9 |
| `vector(n)` (pgvector) | vector kind | unchanged | |

## Coverage: MySQL

| Type | Today | Under this ADR | Needs |
|---|---|---|---|
| `TINYINT`, `SMALLINT`, `MEDIUMINT`, `INT`, and `UNSIGNED` forms | sized kinds | unchanged | |
| `BIGINT` | `int` | unchanged | |
| `BIGINT UNSIGNED`, `SERIAL` | `int`, values above 2^63 refused | `u64` | D5 |
| `INT1` to `INT8`, `MIDDLEINT`, `FLOAT4`, `FLOAT8`, `INTEGER` | `any` except `INTEGER` | the aliased kind | mapping |
| `FLOAT`, `DOUBLE`, `REAL` | `f32`, `float` | unchanged | |
| `DECIMAL(p,s)`, `NUMERIC`, `FIXED` | `decimal`, modifiers dropped | `decimal` for `p <= 28`, `bigdecimal` above | D1, D2 |
| `BIT(n)` | `bytes` | `u64`, wire type `0x10` | D5 |
| `BOOL`, `BOOLEAN` | `bool` | unchanged | |
| `CHAR(n)`, `VARCHAR(n)`, `NCHAR`, `NVARCHAR`, `NATIONAL CHAR`, `CHARACTER VARYING`, `LONG VARCHAR` | `string`, or `any` for the aliases | `string`, length enforced | D1, mapping |
| `TINYTEXT` to `LONGTEXT` | `string` | `string`, byte limit enforced | D1 |
| `BINARY(n)`, `VARBINARY(n)`, `TINYBLOB` to `LONGBLOB`, `LONG VARBINARY` | `bytes` | `bytes`, length enforced, `BINARY` padded with `0x00` | D1 |
| `DATE` | `date` | unchanged | |
| `TIME(fsp)`, `DATETIME(fsp)`, `TIMESTAMP(fsp)` | ADR 0013 kinds, `fsp` dropped | `fsp` rounding | D1 |
| `YEAR` | `u16`, no wire type | `u16`, wire type `0x0d` | D5 |
| `ENUM`, `SET` | ADR 0013 | unchanged | |
| `JSON` | `object` | `any`, JSON values only | D8 |
| `GEOMETRY`, `POINT`, `LINESTRING`, `POLYGON`, `MULTIPOINT`, `MULTILINESTRING`, `MULTIPOLYGON`, `GEOMETRYCOLLECTION`, `GEOMCOLLECTION` | `any` | `geometry<…>`, WKB on the wire | D6 |
| `VECTOR(n)` | `any` | the pgvector kind | mapping |

## Coverage: DynamoDB

| Attribute type | Kind | Needs |
|---|---|---|
| `S` | `string` | |
| `N` | `bigdecimal`, bounded to 38 digits and an exponent of -130 to +125 by the D1 type | D1, D2 |
| `B` | `bytes` | |
| `BOOL` | `bool` | |
| `NULL` | `null` | |
| `M` | `object` | |
| `L` | `array` | |
| `SS` | `set<string>` | |
| `NS` | `set<bigdecimal>` | D2 |
| `BS` | `set<bytes>` | |

DynamoDB's own limits (no empty sets, no empty key strings, 400 KB items, numbers normalised without leading or trailing zeros) belong to the protocol front end, not to the kinds. `set` already deduplicates by value, which is what `SS`, `NS`, and `BS` require.

## Delivery

Each step lands with language tests for DDL, round trips, casts, and ordering, and wire unit tests for both text and binary forms.

1. **Mapping only, no decision needed.** MySQL `BIGINT UNSIGNED` and `SERIAL` to `u64` (verifying the wire metadata), the MySQL type aliases, `VECTOR(n)`, and PostgreSQL `T[][]` (D12, the DDL half).
2. **D1, the declared SQL type.** The catalog field and `catalog-schema.lock`, write-path enforcement for lengths, padding, precision, scale, and fractional seconds, and the report through `atttypmod`, `information_schema`, and both wires. This closes ADR 0013 D14.
3. **D4 and D5** on top of D1: the text-shaped and number-shaped types, their validation, and their wire codecs.
4. **D8**, the JSON column change.
5. **D2**, `bigdecimal`, following the ADR 0011 checklist, and DynamoDB `N`.
6. **D6** geometry codecs, **D7** ranges, **D10** infinity, and the D12 wire half.
7. **D9** domains and composites.

## Upgrade-test plan

- Step 1 changes the kinds new DDL writes. Columns created earlier keep their stored kind; a `BIGINT UNSIGNED` column created before step 1 stays `int` until redefined, and the documentation says so. No persisted format changes.
- Step 2 adds an ignorable catalog field. A pair written by the last release and read by this build proves a field without `sql_type` behaves as before.
- Step 5 adds a value kind and a stored tag. As with ADR 0013, no released binary can write it, so it is covered by language tests and frozen byte fixtures until a release does; then gated pairs are added for an indexed `bigdecimal` field and one in a record id.

## Consequences

- No type of the three systems lowers to `any` by omission. A type this ADR does not list is refused at DDL time with its name in the message, rather than accepted as `any`.
- The declared SQL type becomes the single place a column's dialect type lives, so `information_schema`, `pg_attribute.atttypmod`, `format_type`, and the MySQL column definitions read one source.
- One new number kind, no new crate, and no change to the meaning of any stored value.
