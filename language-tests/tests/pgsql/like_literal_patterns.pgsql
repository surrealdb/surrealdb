/**
[env]
namespace = true
database = true
imports = ["pgsql/_like_cases.surql"]

[test]
reason = "A LIKE pattern written as a literal: % and _ are wildcards, a backslash makes the next character literal, a line break is a character, and characters that mean something to a regular expression mean nothing"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }, { id: 4 }, { id: 5 }, { id: 58 }]"

[[test.results]]
value = "[{ id: 1 }, { id: 3 }, { id: 4 }, { id: 5 }]"

[[test.results]]
value = "[{ id: 6 }]"

[[test.results]]
value = "[{ id: 8 }]"

[[test.results]]
value = "[{ id: 48 }]"

[[test.results]]
value = "[{ id: 49 }]"

[[test.results]]
value = "[{ id: 10 }]"

[[test.results]]
value = "[{ id: 10 }, { id: 11 }, { id: 14 }, { id: 45 }, { id: 53 }]"

[[test.results]]
value = "[{ id: 9 }, { id: 13 }]"

[[test.results]]
value = "[{ id: 16 }]"

[[test.results]]
value = "[{ matched: 12 }]"

[[test.results]]
value = "[{ matched: 16 }]"

[[test.results]]
value = "[{ id: 43 }, { id: 44 }]"

[[test.results]]
value = "[{ matched: 58 }]"

[[test.results]]
value = "[{ matched: 36 }]"

[[test.results]]
value = "[{ matched: 27 }]"

[[test.results]]
value = "[{ matched: 22 }]"
*/

SELECT id FROM like_cases WHERE subject LIKE 'A%' ORDER BY id;
SELECT id FROM like_cases WHERE subject LIKE '%lic%' ORDER BY id;
SELECT id FROM like_cases WHERE subject LIKE '50\%' ORDER BY id;
SELECT id FROM like_cases WHERE subject LIKE 'a\_b' ORDER BY id;
SELECT id FROM like_cases WHERE subject LIKE '\%' ORDER BY id;
SELECT id FROM like_cases WHERE subject LIKE '\_' ORDER BY id;
SELECT id FROM like_cases WHERE subject LIKE 'a\\b' ORDER BY id;
SELECT id FROM like_cases WHERE subject LIKE 'a\\%' ORDER BY id;
SELECT id FROM like_cases WHERE subject LIKE 'a\xb' ORDER BY id;
SELECT id FROM like_cases WHERE subject LIKE 'a.c' ORDER BY id;
SELECT count(*) AS matched FROM like_cases WHERE subject LIKE 'a%b';
SELECT count(*) AS matched FROM like_cases WHERE subject LIKE '_';
SELECT id FROM like_cases WHERE subject LIKE '日_語' ORDER BY id;
SELECT count(*) AS matched FROM like_cases WHERE subject LIKE '%';
SELECT count(*) AS matched FROM like_cases WHERE subject ILIKE 'a%';
SELECT count(*) AS matched FROM like_cases WHERE subject NOT LIKE 'a%';
SELECT count(*) AS matched FROM like_cases WHERE subject NOT ILIKE 'A%';
