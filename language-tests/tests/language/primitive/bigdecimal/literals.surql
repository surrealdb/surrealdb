/**
[test]
reason = "A bigdec literal keeps its digits and scale, its text re-parses to the same value, and type::of names it"

[[test.results]]
value = "[1bigdec, 1.50bigdec, -1.50bigdec, 0.000bigdec, 1500bigdec, 0.00001bigdec, 1000.0001bigdec]"

[[test.results]]
value = "[1234567890123456789012345678901234567890bigdec, 12345678901234567890123456789012345.678901234567890123456789012345bigdec, -0.00000000000000000000000000000000000000001bigdec]"

[[test.results]]
value = "['1.50', '10000000000000000000000000000000000000000', '-0.010']"

[[test.results]]
value = "['1.50', 'bigdecimal', true]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['12345678901234567890123456789012345.678901234567890123456789012345', 'bigdecimal', true]"

[[test.results]]
value = "[true, true, true, true, 1]"

[[test.results]]
value = """{ "1bigdec": 'one' }"""

*/

-- The scale is kept as written, and an exponent is folded into the digits.
[1bigdec, 1.50bigdec, -1.50bigdec, 0.000bigdec, 1.5e3bigdec, 1e-5bigdec, 1_000.000_1bigdec];

-- 40 and 65 significant digits, beyond a decimal.
[
	1234567890123456789012345678901234567890bigdec,
	12345678901234567890123456789012345.678901234567890123456789012345bigdec,
	-0.00000000000000000000000000000000000000001bigdec,
];

-- The text of a value is the literal that wrote it.
[<string> 1.50bigdec, <string> 1e40bigdec, <string> -0.010bigdec];
[1.50bigdec.to_string(), type::of(1.50bigdec), type::is_number(1.50bigdec)];

-- A value parsed back from its own text is identical, scale included.
LET $big = 12345678901234567890123456789012345.678901234567890123456789012345bigdec;
[<string> $big, type::of($big), <string> $big == "12345678901234567890123456789012345.678901234567890123456789012345"];

-- Equal values differ only in scale; equality and set membership ignore it.
[1.50bigdec == 1.5bigdec, 1bigdec == 1, 1bigdec == 1.0dec, 1bigdec == 1f, (<set> [1bigdec, 1.0bigdec, 1.00bigdec]).len()];

-- In an object key position the literal is a plain key.
{ 1bigdec: 'one' };
