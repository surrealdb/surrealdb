/**
[test]
reason = "Casts to and from bigdecimal: widening is exact, narrowing to a decimal or an integer refuses a value out of range"

[[test.results]]
value = "[1bigdec, 1.5bigdec, 0.1bigdec, 255bigdec, -7bigdec, 1.250bigdec]"

[[test.results]]
value = "['bigdecimal', 'bigdecimal', 'bigdecimal', 'bigdecimal']"

[[test.results]]
value = "12345678901234567890123456789012345678901234567890.123456789012345bigdec"

[[test.results]]
error = "Could not cast into `bigdecimal` using input `'not a number'`"

[[test.results]]
error = "Could not cast into `bigdecimal` using input `NaN`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2bigdec"

[[test.results]]
value = "[1.5dec, 'decimal']"

[[test.results]]
error = "Could not cast into `decimal` using input `10000000000000000000000000000000000000000bigdec`"

[[test.results]]
error = "Could not cast into `decimal` using input `0.00000000000000000000000000000001bigdec`"

[[test.results]]
value = "[42, 'int', 255u8]"

[[test.results]]
error = "Could not cast into `int` using input `10000000000000000000000000000000000000000bigdec`"

[[test.results]]
error = "Could not cast into `u8` using input `256bigdec`"

[[test.results]]
value = "[1.5f, 'float']"

*/

-- To bigdecimal from every number kind and from text.
[<bigdecimal> 1, <bigdecimal> 1.5dec, <bigdecimal> 0.1f, <bigdecimal> 255u8, <bigdecimal> -7i16, <bigdecimal> "1.250"];
[type::of(<bigdecimal> 1), type::of(<bigdecimal> 1.5dec), type::of(<bigdecimal> 0.1f), type::of(<bigdecimal> "1.250")];
<bigdecimal> "12345678901234567890123456789012345678901234567890.123456789012345";
<bigdecimal> "not a number";
<bigdecimal> NaN;
<option<bigdecimal>> NONE;
<option<bigdecimal>> 2;

-- Narrowing to decimal: exact within its range, refused beyond it.
[<decimal> 1.50bigdec, type::of(<decimal> 1.50bigdec)];
<decimal> 1e40bigdec;
<decimal> 0.00000000000000000000000000000001bigdec;

-- Narrowing to an integer refuses a fraction or a value beyond the kind.
[<int> 42bigdec, type::of(<int> 42bigdec), <u8> 255bigdec];
<int> 1e40bigdec;
<u8> 256bigdec;

-- To float rounds to the nearest double.
[<float> 1.5bigdec, type::of(<float> 1e40bigdec)];
