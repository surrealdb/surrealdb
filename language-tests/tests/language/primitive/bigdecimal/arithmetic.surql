/**
[test]
reason = "Arithmetic with a bigdecimal operand is exact and a bigdecimal; division rounds half away from zero to max(20, operand scale) + 4 digits"

[[test.results]]
value = "[1234567890123456789012345678901234567891bigdec, 1bigdec, 9999999999999999999999999999999999999998.0000000000000000000000000000000000000001bigdec, 3.305bigdec, -1.50bigdec]"

[[test.results]]
value = "['bigdecimal', 'bigdecimal', 'bigdecimal', 'bigdecimal', 'bigdecimal', 1.5bigdec, 2.5bigdec]"

[[test.results]]
value = "[0.333333333333333333333333bigdec, 0.666666666666666666666667bigdec, -0.666666666666666666666667bigdec, -0.666666666666666666666667bigdec]"

[[test.results]]
value = "[0.125000000000000000000000bigdec, 2.500000000000000000000000bigdec, 0.33333333333333333333333333333bigdec]"

[[test.results]]
value = "NaN"

[[test.results]]
value = "[1.5bigdec, 1267650600228229401496703205376bigdec, 'bigdecimal']"

*/

-- Sums, differences and products are exact at any size.
[
	1234567890123456789012345678901234567890bigdec + 1bigdec,
	1234567890123456789012345678901234567890bigdec - 1234567890123456789012345678901234567889bigdec,
	99999999999999999999.99999999999999999999bigdec * 99999999999999999999.99999999999999999999bigdec,
	1.10bigdec + 2.205bigdec,
	-(1.50bigdec),
];

-- A bigdecimal operand makes the result a bigdecimal, whatever the other kind.
[
	type::of(1bigdec + 1),
	type::of(1bigdec + 1.5dec),
	type::of(1bigdec + 1.5f),
	type::of(1i8 * 2bigdec),
	type::of(1bigdec - 1u64),
	1bigdec + 0.5f,
	1bigdec + 1.5dec,
];

-- Division rounds half away from zero to 24 digits, or the operand scale plus 4 above 20.
[1bigdec / 3bigdec, 2bigdec / 3bigdec, -2bigdec / 3bigdec, 2bigdec / -3];
[1bigdec / 8, 10bigdec / 4bigdec, 1.0000000000000000000000000bigdec / 3];
1bigdec / 0bigdec;

-- Remainder and power stay exact.
[7.5bigdec % 2, 2bigdec ** 100, type::of(2bigdec ** 100)];
