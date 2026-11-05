/**
[test]

[[test.results]]
value = "0i8"

[[test.results]]
value = "127i8"

[[test.results]]
value = "-128i8"

[[test.results]]
value = "-128i8"

[[test.results]]
value = "127i8"

[[test.results]]
value = "0u8"

[[test.results]]
value = "255u8"

[[test.results]]
value = "255u8"

[[test.results]]
value = "0i16"

[[test.results]]
value = "32767i16"

[[test.results]]
value = "-32768i16"

[[test.results]]
value = "-32768i16"

[[test.results]]
value = "0u16"

[[test.results]]
value = "65535u16"

[[test.results]]
value = "1000u16"

[[test.results]]
value = "0i32"

[[test.results]]
value = "2147483647i32"

[[test.results]]
value = "-2147483648i32"

[[test.results]]
value = "-2147483648i32"

[[test.results]]
value = "0u32"

[[test.results]]
value = "4294967295u32"

[[test.results]]
value = "0"

[[test.results]]
value = "42"

[[test.results]]
value = "9223372036854775807"

[[test.results]]
value = "-9223372036854775808"

[[test.results]]
value = "-9223372036854775808"

[[test.results]]
value = "0u64"

[[test.results]]
value = "18446744073709551615u64"

[[test.results]]
value = "9223372036854775808u64"

[[test.results]]
value = "0f16"

[[test.results]]
value = "-0f16"

[[test.results]]
value = "1f16"

[[test.results]]
value = "-1f16"

[[test.results]]
value = "1.5f16"

[[test.results]]
value = "0.1f16"

[[test.results]]
value = "65504f16"

[[test.results]]
value = "-65504f16"

[[test.results]]
value = "15f16"

[[test.results]]
value = "0f32"

[[test.results]]
value = "-0f32"

[[test.results]]
value = "1f32"

[[test.results]]
value = "-1f32"

[[test.results]]
value = "1.5f32"

[[test.results]]
value = "0.1f32"

[[test.results]]
value = "100f32"

[[test.results]]
value = "-150f32"

[[test.results]]
value = "0f"

[[test.results]]
value = "1f"

[[test.results]]
value = "1.5f"

[[test.results]]
value = "1000f"

[[test.results]]
value = "-2.5f"

[[test.results]]
value = "['5u8', '-5i8', '1000u16', '-1000i16', '100000u32', '-100000i32', '18446744073709551615u64', '1.5f16', '0.1f16', '1.5f32']"

[[test.results]]
value = "<f16>NaN"

[[test.results]]
value = "<f16>Infinity"

[[test.results]]
value = "<f16>-Infinity"

[[test.results]]
value = "<f32>NaN"

[[test.results]]
value = "<f32>Infinity"

[[test.results]]
value = "<f32>-Infinity"
*/

-- Sized integer literals and boundaries
0i8;
127i8;
-128i8;
(-128i8);
+127i8;

0u8;
255u8;
+255u8;

0i16;
32767i16;
-32768i16;
(-32768i16);

0u16;
65535u16;
1_000u16;

0i32;
2147483647i32;
-2147483648i32;
(-2147483648i32);

0u32;
4294967295u32;

-- i64 alias produces int (renders bare)
0i64;
42i64;
9223372036854775807i64;
-9223372036854775808i64;
(-9223372036854775808i64);

0u64;
18446744073709551615u64;
9223372036854775808u64;

-- Sized float literals
0f16;
-0f16;
1f16;
-1f16;
1.5f16;
0.1f16;
65504f16;
-65504f16;
1.5e1f16;

0f32;
-0f32;
1f32;
-1f32;
1.5f32;
0.1f32;
1e2f32;
-1.5e2f32;

-- f64 alias produces float (renders with f)
0f64;
1f64;
1.5f64;
1e3f64;
-2.5f64;

-- Rendering via string cast (uses ToSql, preserving suffix)
[<string> 5u8, <string> -5i8, <string> 1000u16, <string> -1000i16, <string> 100000u32, <string> -100000i32, <string> 18446744073709551615u64, <string> 1.5f16, <string> 0.1f16, <string> 1.5f32];

-- Non-finite float casts and rendering
<f16> NaN;
<f16> Infinity;
<f16> -Infinity;
<f32> NaN;
<f32> Infinity;
<f32> -Infinity;
