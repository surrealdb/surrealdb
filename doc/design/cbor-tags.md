# CBOR tag table

The RPC and HTTP endpoints exchange CBOR in two negotiated forms. `cbor` is the frozen form; `cbor.2` adds the tags marked below. This table is the contract between the server and every client library.

| Tag | Value | Payload | `cbor` | `cbor.2` |
|---|---|---|---|---|
| 6 | `NONE` | null | yes | yes |
| 7 | table name | text | yes | yes |
| 8 | record id | `[table, key]` | yes | yes |
| 10 | decimal | text | yes | yes |
| 12 | datetime | `[seconds, nanoseconds]` since the Unix epoch | yes | yes |
| 14 | duration | `[seconds, nanoseconds]`, trailing zero parts omitted | yes | yes |
| 37 | uuid | 16 bytes | yes | yes |
| 49, 50, 51 | range, included bound, excluded bound | | yes | yes |
| 55 | file | `[bucket, key]` | yes | yes |
| 56 | set | array | yes | yes |
| 88 to 94 | geometry | by geometry kind | yes | yes |
| 100 | date | integer days since 1970-01-01 (RFC 8943) | degraded | yes |
| 180 to 188 | `i8`, `u8`, `i16`, `u16`, `i32`, `u32`, `u64`, `f16`, `f32` | integer or float | degraded | yes |
| 189 | time | unsigned integer nanoseconds since midnight, up to 24 hours | degraded | yes |
| 190 | timestamp | `[seconds, nanoseconds]` of wall-clock time since 1970-01-01T00:00:00, no time zone | degraded | yes |
| 191 | interval | `[months, days, microseconds]`, each a signed integer | degraded | yes |
| 192 | bigdecimal | text in positional notation with the value's scale, for example `1.50` or `-0.0000000000000000000000000000001` | degraded | yes |

## Degraded forms in `cbor`

A server never sends a tag marked degraded to a `cbor` client, and refuses to decode one from it.

| Value | Sent as |
|---|---|
| date | datetime (tag 12) at midnight |
| timestamp | datetime (tag 12) read as UTC |
| time | duration (tag 14) since midnight |
| interval | ISO 8601 text, for example `P1Y2M3DT4H5M6.5S` |
| sized numbers | plain integer, float or decimal |
| bigdecimal | decimal (tag 10) when it fits a decimal exactly, else plain text in positional notation |

## Bigdecimal

A bigdecimal that fits a decimal exactly is sent as tag 10 in every format. Any other bigdecimal is sent as tag 192 under `cbor.2`, as plain text under `cbor`, and as a string under FlatBuffers and protobuf, which have no arbitrary-precision decimal. The text of tag 192 must parse as a bigdecimal; tag 192 is refused under `cbor`.

## Ranges

A date outside the range of `chrono::NaiveDate`, a time past `24:00:00`, a timestamp outside the range of `chrono::NaiveDateTime`, and an interval component outside its integer width are refused when decoded.
