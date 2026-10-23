/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL quote_ident quotes anything that is not a bare lowercase identifier or that is a non-unreserved keyword, and passes NULL through, for constants and column values"

[[test.results]]
value = "[{ a: 'foo', b: '\"Foo\"', c: '\"a b\"', d: '\"a\"\"b\"', e: '\"select\"', f: '\"\"', g: NULL, h: '\"table\"', i: 'name', j: '_x1$', k: '\"1x\"' }]"
*/

SELECT quote_ident('foo') AS a, quote_ident('Foo') AS b, quote_ident('a b') AS c, quote_ident('a"b') AS d, quote_ident('select') AS e, quote_ident('') AS f, quote_ident(NULL) AS g, quote_ident('table') AS h, quote_ident('name') AS i, quote_ident('_x1$') AS j, quote_ident('1x') AS k;
