/**
[test]

[[test.results]]
value = "['i8', 'u8', 'i16', 'u16', 'i32', 'u32', 'int', 'int', 'u64', 'f16', 'f32', 'float', 'float', 'decimal']"

[[test.results]]
value = "[true, true, true, true, true, true, true, true, true, true, true, true, false]"

[[test.results]]
value = "[true, false, false, true, false, false, true, true, true, true, true, true, false, false, true, false, true, false, false, true, false, false, true, true, false, true, false]"

[[test.results]]
value = "[42i8, 42u8, 1000i16, 1000u16, 100000i32, 100000u32, 42, 42u64, 1.5f16, 1.5f32, 1.5f, 42dec, 42u8]"

[[test.results]]
error = "Could not cast into `u8` using input `256`"

[[test.results]]
error = "Could not cast into `u8` using input `-1`"

[[test.results]]
error = "Could not cast into `i8` using input `128`"

[[test.results]]
error = "Could not cast into `i8` using input `-129`"

[[test.results]]
error = "Could not cast into `int` using input `18446744073709551615u64`"
*/

-- type::of for every number kind
[
	type::of(5i8),
	type::of(5u8),
	type::of(5i16),
	type::of(5u16),
	type::of(5i32),
	type::of(5u32),
	type::of(5i64),
	type::of(5),
	type::of(5u64),
	type::of(1.5f16),
	type::of(1.5f32),
	type::of(1.5f64),
	type::of(1.5f),
	type::of(1.5dec),
];

-- type::is_number is true for every number kind
[
	type::is_number(5i8),
	type::is_number(5u8),
	type::is_number(5i16),
	type::is_number(5u16),
	type::is_number(5i32),
	type::is_number(5u32),
	type::is_number(5i64),
	type::is_number(5u64),
	type::is_number(1.5f16),
	type::is_number(1.5f32),
	type::is_number(1.5f64),
	type::is_number(1.5dec),
	type::is_number("5"),
];

-- Specific kind predicates test exact kind (D3: type::is_int(5u8) is false)
[
	type::is_i8(5i8),
	type::is_i8(5u8),
	type::is_i8(5),
	type::is_u8(5u8),
	type::is_u8(5i8),
	type::is_u8(5),
	type::is_i16(5i16),
	type::is_u16(5u16),
	type::is_i32(5i32),
	type::is_u32(5u32),
	type::is_int(5),
	type::is_int(5i64),
	type::is_int(5u8),
	type::is_int(5u64),
	type::is_u64(5u64),
	type::is_u64(5),
	type::is_f16(1.5f16),
	type::is_f16(1.5f32),
	type::is_f16(1.5f),
	type::is_f32(1.5f32),
	type::is_f32(1.5f16),
	type::is_f32(1.5f),
	type::is_float(1.5f),
	type::is_float(1.5f64),
	type::is_float(1.5f32),
	type::is_decimal(1.5dec),
	type::is_decimal(1.5f),
];

-- type::<kind>(v) conversion functions
[
	type::i8(42),
	type::u8(42),
	type::i16(1000),
	type::u16(1000),
	type::i32(100000),
	type::u32(100000),
	type::int(42u8),
	type::u64(42),
	type::f16(1.5),
	type::f32(1.5),
	type::float(1.5f32),
	type::decimal(42u8),
	type::number(42u8),
];

-- Out-of-range errors in type::<kind>
type::u8(256);
type::u8(-1);
type::i8(128);
type::i8(-129);
type::int(18446744073709551615u64);
