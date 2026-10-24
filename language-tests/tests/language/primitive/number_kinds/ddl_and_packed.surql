/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ events: {  }, fields: { cf16: 'DEFINE FIELD cf16 ON packed_num TYPE f16 DEFAULT 1.5f16 PERMISSIONS FULL', cf32: 'DEFINE FIELD cf32 ON packed_num TYPE f32 DEFAULT 2.5f32 PERMISSIONS FULL', cf64: 'DEFINE FIELD cf64 ON packed_num TYPE float DEFAULT 3.5f PERMISSIONS FULL', ci16: 'DEFINE FIELD ci16 ON packed_num TYPE i16 DEFAULT -2i16 PERMISSIONS FULL', ci32: 'DEFINE FIELD ci32 ON packed_num TYPE i32 DEFAULT -3i32 PERMISSIONS FULL', ci64: 'DEFINE FIELD ci64 ON packed_num TYPE int DEFAULT -4 PERMISSIONS FULL', ci8: 'DEFINE FIELD ci8 ON packed_num TYPE i8 DEFAULT -1i8 PERMISSIONS FULL', cu16: 'DEFINE FIELD cu16 ON packed_num TYPE u16 DEFAULT 2u16 PERMISSIONS FULL', cu32: 'DEFINE FIELD cu32 ON packed_num TYPE u32 DEFAULT 3u32 PERMISSIONS FULL', cu64: 'DEFINE FIELD cu64 ON packed_num TYPE u64 DEFAULT 4u64 PERMISSIONS FULL', cu8: 'DEFINE FIELD cu8 ON packed_num TYPE u8 DEFAULT 1u8 PERMISSIONS FULL' }, indexes: {  }, lives: {  }, migrations: { generation: 12, guaranteed: [], migrations: [] }, tables: {  } }"

[[test.results]]
value = "[{ cf16: 1.5f16, cf32: 2.5f32, cf64: 3.5f, ci16: -2i16, ci32: -3i32, ci64: -4, ci8: -1i8, cu16: 2u16, cu32: 3u32, cu64: 4u64, cu8: 1u8, id: packed_num:defaults }]"

[[test.results]]
value = "[{ cf16: 65504f16, cf32: 1234.5f32, cf64: -5678.25f, ci16: -32768i16, ci32: -2147483648i32, ci64: -9223372036854775808, ci8: -128i8, cu16: 65535u16, cu32: 4294967295u32, cu64: 18446744073709551615u64, cu8: 255u8, id: packed_num:bounds }]"

[[test.results]]
value = "[{ cf16: 65504f16, cf32: 1234.5f32, cf64: -5678.25f, ci16: -32768i16, ci32: -2147483648i32, ci64: -9223372036854775808, ci8: -128i8, cu16: 65535u16, cu32: 4294967295u32, cu64: 18446744073709551615u64, cu8: 255u8, id: packed_num:bounds }, { cf16: 1.5f16, cf32: 2.5f32, cf64: 3.5f, ci16: -2i16, ci32: -3i32, ci64: -4, ci8: -1i8, cu16: 2u16, cu32: 3u32, cu64: 4u64, cu8: 1u8, id: packed_num:defaults }]"

[[test.results]]
value = "[{ cf16: 65504f16, cf32: 1234.5f32, cf64: -5678.25f, ci16: -32768i16, ci32: -2147483648i32, ci64: -9223372036854775808, ci8: -128i8, cu16: 65535u16, cu32: 4294967295u32, cu64: 18446744073709551615u64, cu8: 255u8, id: packed_num:bounds, tf16: 'f16', tf32: 'f32', tf64: 'float', ti16: 'i16', ti32: 'i32', ti64: 'int', ti8: 'i8', tu16: 'u16', tu32: 'u32', tu64: 'u64', tu8: 'u8' }, { cf16: 1.5f16, cf32: 2.5f32, cf64: 3.5f, ci16: -2i16, ci32: -3i32, ci64: -4, ci8: -1i8, cu16: 2u16, cu32: 3u32, cu64: 4u64, cu8: 1u8, id: packed_num:defaults, tf16: 'f16', tf32: 'f32', tf64: 'float', ti16: 'i16', ti32: 'i32', ti64: 'int', ti8: 'i8', tu16: 'u16', tu32: 'u32', tu64: 'u64', tu8: 'u8' }]"

[[test.results]]
value = "[{ cu8: 255u8, id: packed_num:bounds }]"

[[test.results]]
value = "[{ cu8: 255u8, id: packed_num:bounds }]"

[[test.results]]
value = "[{ cf16: 65504f16, id: packed_num:bounds }]"

[[test.results]]
value = "[{ cu64: 18446744073709551615u64, id: packed_num:bounds }]"

[[test.results]]
value = "[{ cf16: 0.5f16, cf32: 2.5f32, cf64: 3.5f, ci16: -2i16, ci32: -3i32, ci64: -4, ci8: -1i8, cu16: 2u16, cu32: 3u32, cu64: 4u64, cu8: 99u8, id: packed_num:defaults }]"

[[test.results]]
value = "[{ cf16: 0.5f16, cu8: 99u8, id: packed_num:defaults, tf16: 'f16', tu8: 'u8' }]"
*/

-- SCHEMAFULL table with every sized column (packed-row layout) and DEFAULT values
DEFINE TABLE packed_num SCHEMAFULL;
DEFINE FIELD ci8 ON packed_num TYPE i8 DEFAULT -1i8;
DEFINE FIELD cu8 ON packed_num TYPE u8 DEFAULT 1u8;
DEFINE FIELD ci16 ON packed_num TYPE i16 DEFAULT -2i16;
DEFINE FIELD cu16 ON packed_num TYPE u16 DEFAULT 2u16;
DEFINE FIELD ci32 ON packed_num TYPE i32 DEFAULT -3i32;
DEFINE FIELD cu32 ON packed_num TYPE u32 DEFAULT 3u32;
DEFINE FIELD ci64 ON packed_num TYPE i64 DEFAULT -4i64;
DEFINE FIELD cu64 ON packed_num TYPE u64 DEFAULT 4u64;
DEFINE FIELD cf16 ON packed_num TYPE f16 DEFAULT 1.5f16;
DEFINE FIELD cf32 ON packed_num TYPE f32 DEFAULT 2.5f32;
DEFINE FIELD cf64 ON packed_num TYPE f64 DEFAULT 3.5f64;

-- INFO FOR TABLE normalizes i64 -> int and f64 -> float
INFO FOR TABLE packed_num;

-- Create row using defaults and explicit values
CREATE packed_num:defaults;
CREATE packed_num:bounds SET
	ci8 = -128i8,
	cu8 = 255u8,
	ci16 = -32768i16,
	cu16 = 65535u16,
	ci32 = -2147483648i32,
	cu32 = 4294967295u32,
	ci64 = -9223372036854775808i64,
	cu64 = 18446744073709551615u64,
	cf16 = 65504f16,
	cf32 = 1234.5f32,
	cf64 = -5678.25f64;

-- Read whole rows and individual fields, checking preserved kinds
SELECT * FROM packed_num ORDER BY id;
SELECT
	id,
	ci8, type::of(ci8) AS ti8,
	cu8, type::of(cu8) AS tu8,
	ci16, type::of(ci16) AS ti16,
	cu16, type::of(cu16) AS tu16,
	ci32, type::of(ci32) AS ti32,
	cu32, type::of(cu32) AS tu32,
	ci64, type::of(ci64) AS ti64,
	cu64, type::of(cu64) AS tu64,
	cf16, type::of(cf16) AS tf16,
	cf32, type::of(cf32) AS tf32,
	cf64, type::of(cf64) AS tf64
FROM packed_num ORDER BY id;

-- Filter pushdown on packed columns across kinds
SELECT id, cu8 FROM packed_num WHERE cu8 = 255;
SELECT id, cu8 FROM packed_num WHERE cu8 = 255u8;
SELECT id, cf16 FROM packed_num WHERE cf16 > 10f;
SELECT id, cu64 FROM packed_num WHERE cu64 > 9223372036854775807;

-- Update a single packed slot in place
UPDATE packed_num:defaults SET cu8 = 99u8, cf16 = 0.5f16;
SELECT id, cu8, type::of(cu8) AS tu8, cf16, type::of(cf16) AS tf16 FROM packed_num:defaults;
