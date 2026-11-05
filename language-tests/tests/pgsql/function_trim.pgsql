/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL trim, ltrim and rtrim remove spaces (or the given characters) from their own end only, and keep other white space"

[[test.results]]
value = "[{ a: 'pad', b: 'pad  ', c: '  pad' }]"

[[test.results]]
value = "[{ a: 'abcxx', b: 'xxabc', c: '\t pad', d: NULL }]"
*/

SELECT trim('  pad  ') AS a, ltrim('  pad  ') AS b, rtrim('  pad  ') AS c;

SELECT ltrim('xxabcxx', 'x') AS a, rtrim('xxabcxx', 'x') AS b, trim(E'\t pad ') AS c, ltrim(NULL) AS d;
