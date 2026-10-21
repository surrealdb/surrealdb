/**
[env]
namespace = true
database = true

[test]
reason = """
A two-argument `vector::*` builtin called over a field of the row and a literal
or a parameter's vector answers, row by row, what the same call answers over the
field read through an expression (`v ?? NONE`): the same values, of the same
number kinds, and the same errors, in either argument order. The pairs of
statements below answer alike, the second of each pair calling the builtin over
`v ?? NONE`.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ d: 1.5811388300841898f, id: t:3 }, { d: 2.23606797749979f, id: t:2 }, { d: 5f, id: t:1 }]"

[[test.results]]
value = "[{ d: 1.5811388300841898f, id: t:3 }, { d: 2.23606797749979f, id: t:2 }, { d: 5f, id: t:1 }]"

[[test.results]]
value = "[{ d: 7f, id: t:1, int: false }, { d: 3f, id: t:2, int: false }, { d: 2f, id: t:3, int: false }]"

[[test.results]]
value = "[{ d: 7f, id: t:1, int: false }, { d: 3f, id: t:2, int: false }, { d: 2f, id: t:3, int: false }]"

[[test.results]]
value = "[{ d: 7f, id: t:1, int: false }, { d: 3, id: t:2, int: true }, { d: 2f, id: t:3, int: false }]"

[[test.results]]
value = "[{ d: 7f, id: t:1, int: false }, { d: 3, id: t:2, int: true }, { d: 2f, id: t:3, int: false }]"

[[test.results]]
value = "[{ d: NaN, id: t:1 }, { d: NaN, id: t:2 }, { d: NaN, id: t:3 }]"

[[test.results]]
value = "[{ d: NaN, id: t:1 }, { d: NaN, id: t:2 }, { d: NaN, id: t:3 }]"

[[test.results]]
value = "[t:3]"

[[test.results]]
value = "[t:3]"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 1 was the wrong type. Expected `number` but found `[3f]` when coercing element at index 0 of `array<number>`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 1 was the wrong type. Expected `number` but found `[3f]` when coercing element at index 0 of `array<number>`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 2 was the wrong type. Expected `number` but found `'x'` when coercing element at index 1 of `array<number>`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 2 was the wrong type. Expected `number` but found `'x'` when coercing element at index 1 of `array<number>`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::hamming(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::distance::hamming(). The two vectors must be of the same dimension."

[[test.results]]
error = "Incorrect arguments for function vector::similarity::jaccard(). Argument 1 was the wrong type. Expected `array<number>` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function vector::similarity::jaccard(). Argument 1 was the wrong type. Expected `array<number>` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 1 was the wrong type. Expected `array<number>` but found `3`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 1 was the wrong type. Expected `array<number>` but found `3`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 2 was the wrong type. Expected `array<number>` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 2 was the wrong type. Expected `array<number>` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 2 was the wrong type. Expected `number` but found `'a'` when coercing element at index 0 of `array<number>`"

[[test.results]]
error = "Incorrect arguments for function vector::distance::euclidean(). Argument 2 was the wrong type. Expected `number` but found `'a'` when coercing element at index 0 of `array<number>`"

*/
{
	CREATE t:1 SET v = [3f, 4f];
	CREATE t:2 SET v = [1, 2];
	CREATE t:3 SET v = [0.5f, 1.5f];
	CREATE bad:nested SET v = [[3f], [4f]];
	CREATE bad:text SET v = [1f, 'x'];
	CREATE bad:wide SET v = [1f, 2f, 3f];
	CREATE bad:missing SET w = 1;
	CREATE bad:scalar SET v = 3;
	DEFINE PARAM $q VALUE [0f, 0f];
	RETURN NONE;
};
SELECT id, vector::distance::euclidean(v, [0f, 0f]) AS d FROM t ORDER BY d;
SELECT id, vector::distance::euclidean(v ?? NONE, [0f, 0f]) AS d FROM t ORDER BY d;
SELECT id, vector::distance::manhattan($q, v) AS d, type::is_int(vector::distance::manhattan($q, v)) AS int FROM t ORDER BY id;
SELECT id, vector::distance::manhattan($q, v ?? NONE) AS d, type::is_int(vector::distance::manhattan($q, v ?? NONE)) AS int FROM t ORDER BY id;
SELECT id, vector::dot(v, [1, 1]) AS d, type::is_int(vector::dot(v, [1, 1])) AS int FROM t ORDER BY id;
SELECT id, vector::dot(v ?? NONE, [1, 1]) AS d, type::is_int(vector::dot(v ?? NONE, [1, 1])) AS int FROM t ORDER BY id;
SELECT id, vector::similarity::cosine(v, $q) AS d FROM t ORDER BY id;
SELECT id, vector::similarity::cosine(v ?? NONE, $q) AS d FROM t ORDER BY id;
SELECT VALUE id FROM t WHERE vector::distance::chebyshev(v, [0f, 0f]) < 2;
SELECT VALUE id FROM t WHERE vector::distance::chebyshev(v ?? NONE, [0f, 0f]) < 2;
SELECT vector::distance::euclidean(v, [0f, 0f]) FROM bad:nested;
SELECT vector::distance::euclidean(v ?? NONE, [0f, 0f]) FROM bad:nested;
SELECT vector::distance::euclidean([0f, 0f], v) FROM bad:text;
SELECT vector::distance::euclidean([0f, 0f], v ?? NONE) FROM bad:text;
SELECT vector::distance::hamming(v, [0f, 0f]) FROM bad:wide;
SELECT vector::distance::hamming(v ?? NONE, [0f, 0f]) FROM bad:wide;
SELECT vector::similarity::jaccard(v, $q) FROM bad:missing;
SELECT vector::similarity::jaccard(v ?? NONE, $q) FROM bad:missing;
SELECT vector::distance::euclidean(v, [0f, 0f]) FROM bad:scalar;
SELECT vector::distance::euclidean(v ?? NONE, [0f, 0f]) FROM bad:scalar;
SELECT vector::distance::euclidean(v, $undefined) FROM t:1;
SELECT vector::distance::euclidean(v ?? NONE, $undefined) FROM t:1;
SELECT vector::distance::euclidean(v, ['a', 'b']) FROM t:1;
SELECT vector::distance::euclidean(v ?? NONE, ['a', 'b']) FROM t:1;
