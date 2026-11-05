/**
[test]

[[test.results]]
value = "[3i8, 3u8, 3i16, 3u16, 3i32, 3u32, 3, 3u64, 4f16, 4f32, 4f, 4.0dec]"

[[test.results]]
value = "[3i16, 3i32, 3, 3u16, 3u32, 3u64]"

[[test.results]]
value = "[3i16, 3i16, 3i32, 3i32, 3, 3, 3dec, 3dec, 'decimal']"

[[test.results]]
value = "[3.5f16, 3.5f16, 3.5f32, 3.5f32, 3.5f, 3.5f, 3.5f32, 3.5f, 3.5f, 3.5f]"

[[test.results]]
value = "[3.5dec, 3.5dec, 3.0dec, 3.0dec]"

[[test.results]]
error = "Cannot perform addition with '127i8' and '1i8'"

[[test.results]]
error = "Cannot perform subtraction with '-128i8' and '1i8'"

[[test.results]]
error = "Cannot perform addition with '255u8' and '1u8'"

[[test.results]]
error = "Cannot perform subtraction with '0u8' and '1u8'"

[[test.results]]
error = "Cannot perform multiplication with '100u8' and '3u8'"

[[test.results]]
error = "Cannot perform addition with '32767i16' and '1i16'"

[[test.results]]
error = "Cannot perform addition with '65535u16' and '1u16'"

[[test.results]]
error = "Cannot perform addition with '2147483647i32' and '1i32'"

[[test.results]]
error = "Cannot perform addition with '4294967295u32' and '1u32'"

[[test.results]]
error = "Cannot perform addition with '18446744073709551615u64' and '1u64'"

[[test.results]]
value = "300u16"

[[test.results]]
value = "128i16"

[[test.results]]
value = "256i16"

[[test.results]]
value = "[3u8, 'u8', 3.5f16, 'f16', 3.5f32, 'f32', 7dec, 'decimal']"

[[test.results]]
value = "NaN"

[[test.results]]
value = "<f16>Infinity"

[[test.results]]
value = "<f32>Infinity"

[[test.results]]
value = "[3u8, 3i16, 1.5f16, 1.5f32]"

[[test.results]]
error = "Cannot perform remainder with '7u8' and '0u8'"

[[test.results]]
value = "[-5i8, -5i16, -5i32, -5, -1.5f16, -1.5f32, -1.5f, 0u8, 0u16, 0u32, 0u64]"

[[test.results]]
error = "Cannot negate the value '-128i8'"

[[test.results]]
error = "Cannot negate the value '-32768i16'"

[[test.results]]
error = "Cannot negate the value '-2147483648i32'"

[[test.results]]
error = "Cannot negate the value '1u8'"

[[test.results]]
error = "Cannot negate the value '1u16'"

[[test.results]]
error = "Cannot negate the value '1u32'"

[[test.results]]
error = "Cannot negate the value '1u64'"

[[test.results]]
value = "[8u8, 1024i16, 8f16, 8f32]"

[[test.results]]
error = "Cannot raise the value '2u8' with '8u8'"

[[test.results]]
error = "Cannot raise the value '2i8' with '-1i8'"
*/

-- Same-kind addition preserves kind
[
	1i8 + 2i8,
	1u8 + 2u8,
	1i16 + 2i16,
	1u16 + 2u16,
	1i32 + 2i32,
	1u32 + 2u32,
	1 + 2,
	1u64 + 2u64,
	1.5f16 + 2.5f16,
	1.5f32 + 2.5f32,
	1.5f + 2.5f,
	1.5dec + 2.5dec,
];

-- Same-sign integer join widens to max width
[
	1i8 + 2i16,
	1i16 + 2i32,
	1i32 + 2,
	1u8 + 2u16,
	1u16 + 2u32,
	1u32 + 2u64,
];

-- Mixed-sign integer join widens to smallest signed kind containing both ranges (or decimal for u64 + signed)
[
	1u8 + 2i8,
	1u8 + 2i16,
	1u16 + 2i16,
	1u16 + 2i32,
	1u32 + 2i32,
	1u32 + 2,
	1u64 + 2i8,
	1u64 + 2,
	type::of(1u64 + 2),
];

-- Float + integer join
[
	1.5f16 + 2i8,
	1.5f16 + 2u8,
	1.5f16 + 2i16,
	1.5f16 + 2u16,
	1.5f16 + 2i32,
	1.5f16 + 2,
	1.5f32 + 2i16,
	1.5f32 + 2i32,
	1.5f32 + 2u64,
	1.5f + 2u64,
];

-- Decimal dominates everything
[
	1.5dec + 2u8,
	1.5dec + 2u64,
	1.5dec + 1.5f16,
	1.5dec + 1.5f32,
];

-- Checked integer overflow errors at the joined width
127i8 + 1i8;
-128i8 - 1i8;
255u8 + 1u8;
0u8 - 1u8;
100u8 * 3u8;
32767i16 + 1i16;
65535u16 + 1u16;
2147483647i32 + 1i32;
4294967295u32 + 1u32;
18446744073709551615u64 + 1u64;

-- Mixed-width does not overflow when the joined width fits
200u8 + 100u16;
127i8 + 1i16;
255u8 + 1i8;

-- Division `/` promotes integer operands to float (or decimal when joined to decimal)
[
	7u8 / 2u8,
	type::of(7u8 / 2u8),
	7f16 / 2f16,
	type::of(7f16 / 2f16),
	7f32 / 2f32,
	type::of(7f32 / 2f32),
	7u64 / 1i8,
	type::of(7u64 / 1i8),
];
1u8 / 0u8;
1f16 / 0f16;
1f32 / 0f32;

-- Remainder `%`
[7u8 % 4u8, 7i16 % 4u8, 7.5f16 % 2.0f16, 7.5f32 % 2.0f32];
7u8 % 0u8;

-- Unary negation
[-5i8, -5i16, -5i32, -5, -1.5f16, -1.5f32, -1.5f, -0u8, -0u16, -0u32, -0u64];
-(-128i8);
-(-32768i16);
-(-2147483648i32);
-(1u8);
-(1u16);
-(1u32);
-(1u64);

-- Power `**`
[2u8 ** 3u8, 2i16 ** 10u8, 2.0f16 ** 3u8, 2.0f32 ** 3u8];
2u8 ** 8u8;
2i8 ** -1i8;
