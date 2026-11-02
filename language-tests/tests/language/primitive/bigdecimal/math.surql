/**
[test]
reason = "abs, ceil, floor, round, fixed, sign, min, max, sum, product and mean are exact on a bigdecimal; the other math functions compute in decimal and refuse a value beyond it"

[[test.results]]
value = "[1234567890123456789012345678901234567890.5bigdec, 1234567890123456789012345678901234567891bigdec, -1234567890123456789012345678901234567891bigdec, 3bigdec, -3bigdec, 1234567890123456789012345678901234567891bigdec, 1.234567890123456789012345678901bigdec, 1.3bigdec, -1.3bigdec, -1bigdec, 0bigdec]"

[[test.results]]
value = "['bigdecimal', 'bigdecimal', 'bigdecimal', 'bigdecimal', 'bigdecimal']"

[[test.results]]
value = "[10000000000000000000000000000000000000001.5bigdec, 20000000000000000000000000000000000000000bigdec, -10000000000000000000000000000000000000000bigdec, 10000000000000000000000000000000000000000bigdec, 1.500000000000000000000000bigdec, 1.500000000000000000000000bigdec, 20000000000000000000000000000000000000000.000000000000000000000000bigdec, 10000000000000000000000000000000000000001bigdec]"

[[test.results]]
value = "['bigdecimal', 'int', 'bigdecimal']"

[[test.results]]
value = "[2dec, 1.5000000000000000000000000000dec, 0f, 8bigdec, 3bigdec]"

[[test.results]]
error = "Incorrect arguments for function math::sqrt(). The bigdecimal 10000000000000000000000000000000000000000bigdec is outside the range of a decimal, which this function computes with."

[[test.results]]
error = "Incorrect arguments for function math::sin(). The bigdecimal 10000000000000000000000000000000000000000bigdec is outside the range of a decimal, which this function computes with."

[[test.results]]
error = "Incorrect arguments for function math::log(). The bigdecimal 10000000000000000000000000000000000000000bigdec is outside the range of a decimal, which this function computes with."

[[test.results]]
error = "Incorrect arguments for function math::lerp(). The bigdecimal 10000000000000000000000000000000000000000bigdec is outside the range of a decimal, which this function computes with."

[[test.results]]
value = "[{ g: 'a', id: p:1, v: 10000000000000000000000000000000000000000bigdec }]"

[[test.results]]
value = "[{ g: 'a', id: p:2, v: 1bigdec }]"

[[test.results]]
value = "[{ g: 'b', id: p:3, v: 0.1bigdec }]"

[[test.results]]
value = "[{ g: 'b', id: p:4, v: 0.2bigdec }]"

[[test.results]]
value = "[{ g: 'a', hi: 10000000000000000000000000000000000000000bigdec, lo: 1bigdec, m: 5000000000000000000000000000000000000000.500000000000000000000000bigdec, s: 10000000000000000000000000000000000000001bigdec }, { g: 'b', hi: 0.2bigdec, lo: 0.1bigdec, m: 0.150000000000000000000000bigdec, s: 0.3bigdec }]"

*/

-- Exact single-value functions, at 40 digits and beyond.
[
	math::abs(-1234567890123456789012345678901234567890.5bigdec),
	math::ceil(1234567890123456789012345678901234567890.1bigdec),
	math::floor(-1234567890123456789012345678901234567890.1bigdec),
	math::round(2.5bigdec),
	math::round(-2.5bigdec),
	math::round(1234567890123456789012345678901234567890.5bigdec),
	math::fixed(1.23456789012345678901234567890123456789bigdec, 30),
	math::fixed(1.25bigdec, 1),
	math::fixed(-1.25bigdec, 1),
	math::sign(-1e40bigdec),
	math::sign(0bigdec),
];
[
	type::of(math::abs(-1bigdec)),
	type::of(math::ceil(1.5bigdec)),
	type::of(math::round(1.5bigdec)),
	type::of(math::fixed(1.5bigdec, 2)),
	type::of(math::sign(1bigdec)),
];

-- Exact aggregates over bigdecimals and mixed kinds.
[
	math::sum([1e40bigdec, 1, 0.5dec]),
	math::product([1e20bigdec, 1e20bigdec, 2]),
	math::min([3bigdec, 1e40bigdec, -1e40bigdec, 2]),
	math::max([3bigdec, 1e40bigdec, -1e40bigdec, 2]),
	math::mean([1bigdec, 2bigdec]),
	math::mean([1.0bigdec, 2.0bigdec]),
	math::mean([1e40bigdec, 3e40bigdec]),
	math::spread([1e40bigdec, -1bigdec]),
];
[type::of(math::sum([1bigdec, 2])), type::of(math::max([1bigdec, 2])), type::of(math::max([2.5bigdec, 2]))];

-- The other functions compute in decimal: a bigdecimal within its range works as a decimal does.
[math::sqrt(4bigdec), math::sqrt(2.25bigdec), math::ln(1bigdec), math::pow(2bigdec, 3), math::clamp(5bigdec, 1, 3)];
math::sqrt(1e40bigdec);
math::sin(1e40bigdec);
math::log(1e40bigdec, 10);
math::lerp(0, 1e40bigdec, 0.5);

-- GROUP BY aggregates keep the exact sum.
CREATE p:1 SET g = 'a', v = 1e40bigdec;
CREATE p:2 SET g = 'a', v = 1bigdec;
CREATE p:3 SET g = 'b', v = 0.1bigdec;
CREATE p:4 SET g = 'b', v = 0.2bigdec;
SELECT g, math::sum(v) AS s, math::min(v) AS lo, math::max(v) AS hi, math::mean(v) AS m FROM p GROUP BY g ORDER BY g;
