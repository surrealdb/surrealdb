/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL btrim, strpos, translate, lpad, rpad, array_length, string_to_array, quote_literal, quote_nullable, to_timestamp, stddev and variance"

[[test.results]]
value = "[{ a: 'pad', b: 'abc', c: 4, d: 0, e: 1, f: 'a2x5' }]"

[[test.results]]
value = "[{ a: 'xyxhi', b: '   hi', c: 'hel', d: 'hi', e: '', f: 'hixyx', g: NULL }]"

[[test.results]]
value = "[{ a: ['a', 'b', 'c'], b: ['abc'], c: ['a', 'b', 'c'], d: [], e: 3, f: NULL }]"

[[test.results]]
value = '''[{ a: "'it''s'", b: NULL, c: 'NULL', d: "'x'", e: "E'a\\\\b'" }]'''

[[test.results]]
value = "[{ a: true, b: true }]"

[[test.results]]
value = "[{ a: 1f, b: 1f, c: 1f, d: 1f }]"
*/

SELECT btrim('  pad  ') AS a, btrim('xxabcxx', 'x') AS b, strpos('Hello', 'lo') AS c, strpos('Hello', 'LO') AS d, strpos('Hello', '') AS e, translate('12345', '143', 'ax') AS f;

SELECT lpad('hi', 5, 'xy') AS a, lpad('hi', 5) AS b, lpad('hello', 3) AS c, lpad('hi', 5, '') AS d, lpad('hi', -1) AS e, rpad('hi', 5, 'xy') AS f, rpad(NULL, 3) AS g;

SELECT string_to_array('a,b,c', ',') AS a, string_to_array('abc', '') AS b, string_to_array('abc', NULL) AS c, string_to_array('', ',') AS d, array_length(string_to_array('a,b,c', ','), 1) AS e, array_length(string_to_array('a,b,c', ','), 2) AS f;

SELECT quote_literal('it''s') AS a, quote_literal(NULL) AS b, quote_nullable(NULL) AS c, quote_nullable('x') AS d, quote_literal(E'a\\b') AS e;

SELECT to_timestamp(0) = CAST('1970-01-01T00:00:00Z' AS timestamptz) AS a, to_timestamp(1.5) = CAST('1970-01-01T00:00:01.5Z' AS timestamptz) AS b;

SELECT stddev(n) AS a, variance(n) AS b, stddev_samp(n) AS c, var_samp(n) AS d FROM (VALUES (1), (2), (3)) AS t(n);
