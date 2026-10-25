/**
[env]
namespace = true
database = true
imports = ["pgsql/_like_cases.surql"]

[test]
reason = "A LIKE pattern that is not a literal, a column here and the same shape as a bound parameter, is read as a pattern: % and _ are wildcards, a backslash makes the next character literal, a line break is a character, characters that mean something to a regular expression mean nothing, and NULL gives NULL"

[[test.results]]
value = "[{ id: 2 }, { id: 5 }, { id: 7 }, { id: 9 }, { id: 12 }, { id: 14 }, { id: 15 }, { id: 17 }, { id: 20 }, { id: 22 }, { id: 24 }, { id: 26 }, { id: 28 }, { id: 30 }, { id: 32 }, { id: 35 }, { id: 38 }, { id: 41 }, { id: 42 }, { id: 51 }, { id: 55 }, { id: 58 }]"

[[test.results]]
value = "[{ matched: 35 }]"

[[test.results]]
value = "[{ id: 2 }, { id: 7 }, { id: 9 }, { id: 12 }, { id: 14 }, { id: 15 }, { id: 17 }, { id: 20 }, { id: 22 }, { id: 24 }, { id: 26 }, { id: 28 }, { id: 30 }, { id: 32 }, { id: 35 }, { id: 38 }, { id: 41 }, { id: 42 }, { id: 51 }, { id: 55 }]"

[[test.results]]
value = "[{ matched: 37 }]"

[[test.results]]
value = "[{ id: 2 }, { id: 5 }, { id: 7 }, { id: 9 }, { id: 12 }, { id: 14 }, { id: 15 }, { id: 17 }, { id: 20 }, { id: 22 }, { id: 24 }, { id: 26 }, { id: 28 }, { id: 30 }, { id: 32 }, { id: 42 }, { id: 51 }, { id: 55 }, { id: 58 }]"

[[test.results]]
value = "[{ matched: 38 }]"

[[test.results]]
value = "[{ id: 100, m: NULL, n: NULL }, { id: 101, m: NULL, n: NULL }]"
*/

SELECT id FROM like_cases WHERE subject NOT LIKE pattern ORDER BY id;
SELECT count(*) AS matched FROM like_cases WHERE subject LIKE pattern;
SELECT id FROM like_cases WHERE subject NOT ILIKE pattern ORDER BY id;
SELECT count(*) AS matched FROM like_cases WHERE subject ILIKE pattern;
SELECT id FROM like_cases WHERE id < 98 AND subject NOT LIKE pattern || '%' ORDER BY id;
SELECT count(*) AS matched FROM like_cases WHERE id < 98 AND subject LIKE pattern || '%';
SELECT id, subject LIKE pattern AS m, subject NOT LIKE pattern AS n FROM like_cases WHERE id > 98 ORDER BY id;
