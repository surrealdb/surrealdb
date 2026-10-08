/**
[test]
reason = """
A `SELECT` judges and projects one row before it judges the next, so when the
projection fails on an earlier row than the one the `WHERE` fails on, the
statement fails with the projection's error. A statement that judged a whole
batch of rows before projecting any of them would report the later row's `WHERE`
error instead.
"""

[[test.results]]
value = "[{ a: 1, id: t:1, s: 'x' }]"

[[test.results]]
value = "[{ a: 'bad', id: t:2, s: 'y' }]"

[[test.results]]
error = "Incorrect arguments for function string::len(). Argument 1 was the wrong type. Expected `string` but found `1`"

[[test.results]]
error = "Incorrect arguments for function math::abs(). Argument 1 was the wrong type. Expected `number` but found `'x'`"
*/

CREATE t:1 SET a = 1, s = 'x';
CREATE t:2 SET a = 'bad', s = 'y';
SELECT VALUE string::len(a) FROM t WHERE math::abs(a) > 0;
SELECT VALUE math::abs(s) FROM t WHERE math::abs(a) > 0;
