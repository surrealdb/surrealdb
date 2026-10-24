/**
[test]

[[test.results]]
value = "[127i8, 0i8, -128i8]"

[[test.results]]
value = "[127u8, 255u8, 0u8]"

[[test.results]]
value = "[-128i16, 255i16, 32767i16]"

[[test.results]]
value = "[255u16, 65535u16, 0u16]"

[[test.results]]
value = "[-32768i32, 65535i32, 2147483647i32]"

[[test.results]]
value = "[65535u32, 4294967295u32]"

[[test.results]]
value = "[42, 42, -42]"

[[test.results]]
value = "[42u64, 42u64, 4294967295u64]"

[[test.results]]
value = "[42f16, -42f16, 1000f16]"

[[test.results]]
value = "[42f32, -100000f32, 1000000f32]"

[[test.results]]
value = "[42f, 42f, 18446744073709552000f]"

[[test.results]]
value = "[42dec, -128dec, 18446744073709551615dec]"

[[test.results]]
value = "[42u8, -42i8, 1000u16, 100u64]"

[[test.results]]
value = "[1.5f16, 0.1f16, 1.5f16, 1.5f32, 0.1f32, 1.5f, 1.5f]"

[[test.results]]
error = "Could not cast into `f16` using input `100000f`"

[[test.results]]
error = "Could not cast into `f16` using input `-100000f`"

[[test.results]]
value = "[42i8, -128i8, 127i8]"

[[test.results]]
value = "[0u8, 255u8]"

[[test.results]]
value = "[-32768i16, 32767i16]"

[[test.results]]
value = "[0u16, 65535u16]"

[[test.results]]
value = "[-2147483648i32, 2147483647i32]"

[[test.results]]
value = "[0u32, 4294967295u32]"

[[test.results]]
value = "[0u64, 18446744073709551615u64]"

[[test.results]]
value = "[1.5f16, 0.1f16, <f16>NaN, <f16>Infinity, <f16>-Infinity]"

[[test.results]]
value = "[1.5f32, 0.1f32, <f32>NaN, <f32>Infinity, <f32>-Infinity]"

[[test.results]]
value = "[42, -42, 1.5f]"

[[test.results]]
error = "Could not cast into `u8` using input `256`"

[[test.results]]
error = "Could not cast into `u8` using input `-1`"

[[test.results]]
error = "Could not cast into `i8` using input `128`"

[[test.results]]
error = "Could not cast into `i8` using input `-129`"

[[test.results]]
error = "Could not cast into `i16` using input `32768`"

[[test.results]]
error = "Could not cast into `u16` using input `65536`"

[[test.results]]
error = "Could not cast into `i32` using input `2147483648`"

[[test.results]]
error = "Could not cast into `u32` using input `4294967296`"

[[test.results]]
error = "Could not cast into `int` using input `18446744073709551615u64`"

[[test.results]]
error = "Could not cast into `u64` using input `-1`"

[[test.results]]
error = "Could not cast into `u8` using input `42.9f`"

[[test.results]]
error = "Could not cast into `i8` using input `-42.9f32`"

[[test.results]]
error = "Could not cast into `u16` using input `1000.5dec`"

[[test.results]]
error = "Could not cast into `u8` using input `<f32>NaN`"

[[test.results]]
error = "Could not cast into `i32` using input `<f16>Infinity`"

[[test.results]]
error = "Could not cast into `decimal` using input `<f16>NaN`"

[[test.results]]
error = "Could not cast into `decimal` using input `<f32>Infinity`"

[[test.results]]
error = "Could not cast into `u8` using input `'42i8'`"

[[test.results]]
error = "Could not cast into `i32` using input `'42u32'`"

[[test.results]]
error = "Could not cast into `f16` using input `'1.5f32'`"

[[test.results]]
error = "Could not cast into `f16` using input `'70000'`"

[[test.results]]
error = "Could not cast into `u8` using input `'300'`"
*/

-- Valid casts between integer kinds
[<i8> 127u8, <i8> 0u64, <i8> -128i32];
[<u8> 127i8, <u8> 255u16, <u8> 0i64];
[<i16> -128i8, <i16> 255u8, <i16> 32767u32];
[<u16> 255u8, <u16> 65535i32, <u16> 0i8];
[<i32> -32768i16, <i32> 65535u16, <i32> 2147483647u64];
[<u32> 65535u16, <u32> 4294967295i64];
[<i64> 42u8, <int> 42u32, <int> -42i8];
[<u64> 42i8, <u64> 42, <u64> 4294967295u32];

-- Integer to float and decimal casts
[<f16> 42u8, <f16> -42i8, <f16> 1000i16];
[<f32> 42u8, <f32> -100000i32, <f32> 1000000u64];
[<f64> 42u8, <float> 42u32, <float> 18446744073709551615u64];
[<decimal> 42u8, <decimal> -128i8, <decimal> 18446744073709551615u64];

-- Float / decimal to integer casts (lossless whole-number conversions succeed)
[<u8> 42.0f, <i8> -42.0f32, <u16> 1000.0dec, <u64> 100.0f16];

-- Float to float casts (rounding in range, overflow to error when finite exceeds max)
[<f16> 1.5f, <f16> 0.1f, <f16> 1.5f32, <f32> 1.5f16, <f32> 0.1f, <float> 1.5f16, <float> 1.5f32];
<f16> 100000.0f;
<f16> -100000.0f;

-- String casts: parsed with the target type's parser and range
[<i8> "42", <i8> "-128", <i8> "127"];
[<u8> "0", <u8> "255"];
[<i16> "-32768", <i16> "32767"];
[<u16> "0", <u16> "65535"];
[<i32> "-2147483648", <i32> "2147483647"];
[<u32> "0", <u32> "4294967295"];
[<u64> "0", <u64> "18446744073709551615"];
[<f16> "1.5", <f16> "0.1", <f16> "NaN", <f16> "Infinity", <f16> "-Infinity"];
[<f32> "1.5", <f32> "0.1", <f32> "NaN", <f32> "Infinity", <f32> "-Infinity"];
[<number> "42", <number> "-42", <number> "1.5"];

-- Cast errors: out-of-range integers
<u8> 256;
<u8> -1;
<i8> 128;
<i8> -129;
<i16> 32768;
<u16> 65536;
<i32> 2147483648;
<u32> 4294967296;
<int> 18446744073709551615u64;
<u64> -1;

-- Cast errors: fractional float / decimal to integer
<u8> 42.9f;
<i8> -42.9f32;
<u16> 1000.5dec;

-- Cast errors: non-finite float to integer or decimal
<u8> (<f32> NaN);
<i32> (<f16> Infinity);
<decimal> (<f16> NaN);
<decimal> (<f32> Infinity);

-- Cast errors: suffixed or out-of-range string cast
<u8> "42i8";
<i32> "42u32";
<f16> "1.5f32";
<f16> "70000";
<u8> "300";
