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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, varr: [1u16, 2u16, 65535u16], vdec: 200dec, vf16: 1.5f16, vf32: 1.25f32, vf64: 1.5f, vi16: -32768i16, vi32: -2147483648i32, vi64: 42, vi8: -128i8, vnum: 42u8, vopt: 10u8, vu16: 65535u16, vu32: 4294967295u32, vu64: 18446744073709551615u64, vu8: 255u8 }]"

[[test.results]]
value = "[{ id: t:2, varr: [], vdec: 1.5dec, vf16: 0.1f16, vf32: 0.1f32, vf64: 4294967295f, vi16: 300i16, vi32: 500i32, vi64: 700, vi8: 10i8, vnum: 1.5f32, vu16: 400u16, vu32: 600u32, vu64: 800u64, vu8: 20u8 }]"

[[test.results]]
error = "Couldn't coerce value for field `vu8` of `t:bad_u8`: Expected `u8` but found `256`"

[[test.results]]
error = "Couldn't coerce value for field `vu8` of `t:bad_u8_neg`: Expected `u8` but found `-1`"

[[test.results]]
error = "Couldn't coerce value for field `vi8` of `t:bad_i8`: Expected `i8` but found `128`"

[[test.results]]
error = "Couldn't coerce value for field `vi64` of `t:bad_i64`: Expected `int` but found `9223372036854775808u64`"

[[test.results]]
error = "Couldn't coerce value for field `vu8` of `t:bad_frac`: Expected `u8` but found `1.5f`"

[[test.results]]
error = "Couldn't coerce value for field `vf16` of `t:bad_f16_range`: Expected `f16` but found `65536`"

[[test.results]]
error = "Couldn't coerce value for field `vf32` of `t:bad_f32_range`: Expected `f32` but found `10000000000000000000000000000000000000000f`"

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
value = "[{ a: 5, b: 5, c: 5i8, d: 1.5f, id: u:1 }]"

[[test.results]]
value = "[{ a: 300, b: 5u8, c: 200u8, d: 0.1f, id: u:2 }]"

[[test.results]]
value = "[{ a: -1, b: -1, c: -1i8, d: 1.5f16, id: u:3 }]"

[[test.results]]
value = "[{ a: 5, b: 5, c: 5i8, d: 1.5f, id: u:1, ta: 'int', tb: 'int', tc: 'i8', td: 'float' }, { a: 300, b: 5u8, c: 200u8, d: 0.1f, id: u:2, ta: 'int', tb: 'u8', tc: 'u8', td: 'float' }, { a: -1, b: -1, c: -1i8, d: 1.5f16, id: u:3, ta: 'int', tb: 'int', tc: 'i8', td: 'f16' }]"
*/

DEFINE TABLE t SCHEMAFULL;
DEFINE FIELD vi8 ON t TYPE i8 DEFAULT 0i8;
DEFINE FIELD vu8 ON t TYPE u8 DEFAULT 0u8;
DEFINE FIELD vi16 ON t TYPE i16 DEFAULT 0i16;
DEFINE FIELD vu16 ON t TYPE u16 DEFAULT 0u16;
DEFINE FIELD vi32 ON t TYPE i32 DEFAULT 0i32;
DEFINE FIELD vu32 ON t TYPE u32 DEFAULT 0u32;
DEFINE FIELD vi64 ON t TYPE i64 DEFAULT 0;
DEFINE FIELD vu64 ON t TYPE u64 DEFAULT 0u64;
DEFINE FIELD vf16 ON t TYPE f16 DEFAULT 0f16;
DEFINE FIELD vf32 ON t TYPE f32 DEFAULT 0f32;
DEFINE FIELD vf64 ON t TYPE f64 DEFAULT 0f;
DEFINE FIELD vdec ON t TYPE decimal DEFAULT 0dec;
DEFINE FIELD vnum ON t TYPE number DEFAULT 0;
DEFINE FIELD vopt ON t TYPE option<u8>;
DEFINE FIELD varr ON t TYPE array<u16> DEFAULT [];

-- Exact in-range coercion from bare int/float literals into sized fields
CREATE t:1 SET
	vi8 = -128,
	vu8 = 255,
	vi16 = -32768,
	vu16 = 65535,
	vi32 = -2147483648,
	vu32 = 4294967295,
	vi64 = 42u8,
	vu64 = 18446744073709551615u64,
	vf16 = 1.5,
	vf32 = 1.25,
	vf64 = 1.5f16,
	vdec = 200u8,
	vnum = 42u8,
	vopt = 10,
	varr = [1, 2u8, 65535];

-- Whole integer float / decimal coerces to sized int; f16/f32 round to nearest
CREATE t:2 SET
	vi8 = 10.0f,
	vu8 = 20.0dec,
	vi16 = 300.0f32,
	vu16 = 400.0f16,
	vi32 = 500,
	vu32 = 600,
	vi64 = 700,
	vu64 = 800,
	vf16 = 0.1f,
	vf32 = 0.1f,
	vf64 = 4294967295u32,
	vdec = 1.5f16,
	vnum = 1.5f32,
	vopt = NONE,
	varr = [];

-- Out-of-range integer coercion is refused
CREATE t:bad_u8 SET vu8 = 256;
CREATE t:bad_u8_neg SET vu8 = -1;
CREATE t:bad_i8 SET vi8 = 128;
CREATE t:bad_i64 SET vi64 = 9223372036854775808u64;

-- Fractional float/decimal to integer coercion is refused
CREATE t:bad_frac SET vu8 = 1.5f;

-- Finite value that rounds to infinity in f16/f32 is refused
CREATE t:bad_f16_range SET vf16 = 65536;
CREATE t:bad_f32_range SET vf32 = 1e40;

-- Union type preference: exact kind match wins; otherwise first coercible arm in declaration order
DEFINE TABLE u SCHEMALESS;
DEFINE FIELD a ON u TYPE u8 | int;
DEFINE FIELD b ON u TYPE int | u8;
DEFINE FIELD c ON u TYPE i8 | u8;
DEFINE FIELD d ON u TYPE f16 | float;

CREATE u:1 SET a = 5, b = 5, c = 5, d = 1.5;
CREATE u:2 SET a = 300, b = 5u8, c = 200, d = 0.1;
CREATE u:3 SET a = -1, b = -1, c = -1, d = 1.5f16;
SELECT id, a, type::of(a) AS ta, b, type::of(b) AS tb, c, type::of(c) AS tc, d, type::of(d) AS td FROM u ORDER BY id;
