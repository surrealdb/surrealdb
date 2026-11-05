/**
[env]
namespace = true
database = true
imports = ["pgsql/_scalar_rows.surql"]

[test]
reason = "A subquery used as a value that has a LIMIT of its own keeps it and is an error only when that LIMIT leaves more than one row: LIMIT 1 answers its row over several rows, LIMIT 2 and LIMIT 5 over several rows fail like a subquery with no LIMIT, and a LIMIT over a single row, or with an OFFSET that leaves one, does not fail"

[[test.results]]
error = "An error occurred: more than one row returned by a subquery used as an expression"

[[test.results]]
value = "[{ limit_1: 2 }]"

[[test.results]]
error = "An error occurred: more than one row returned by a subquery used as an expression"

[[test.results]]
error = "An error occurred: more than one row returned by a subquery used as an expression"

[[test.results]]
value = "[{ limit_5_one_row: 5 }]"

[[test.results]]
value = "[{ limit_5_offset_2: 5 }]"

[[test.results]]
error = "An error occurred: more than one row returned by a subquery used as an expression"

[[test.results]]
value = "[{ limit_1_offset_2: 5 }]"

[[test.results]]
value = "[{ limit_0: NONE }]"

[[test.results]]
error = "An error occurred: more than one row returned by a subquery used as an expression"

[[test.results]]
value = "[{ name: 'bob' }]"

[[test.results]]
error = "An error occurred: more than one row returned by a subquery used as an expression"

[[test.results]]
error = "An error occurred: more than one row returned by a subquery used as an expression"

[[test.results]]
value = "[{ name: 'bob' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ c1: NONE, name: 'ada' }, { c1: 2, name: 'bob' }]"

[[test.results]]
error = "An error occurred: more than one row returned by a subquery used as an expression"

[[test.results]]
error = "An error occurred: more than one row returned by a subquery used as an expression"
*/

SELECT (SELECT k2 FROM sr_b) AS no_limit;
SELECT (SELECT k2 FROM sr_b ORDER BY k2 LIMIT 1) AS limit_1;
SELECT (SELECT k2 FROM sr_b ORDER BY k2 LIMIT 2) AS limit_2;
SELECT (SELECT k2 FROM sr_b ORDER BY k2 LIMIT 5) AS limit_5;
SELECT (SELECT k2 FROM sr_b WHERE label = 'eve' LIMIT 5) AS limit_5_one_row;
SELECT (SELECT k2 FROM sr_b ORDER BY k2 LIMIT 5 OFFSET 2) AS limit_5_offset_2;
SELECT (SELECT k2 FROM sr_b ORDER BY k2 LIMIT 5 OFFSET 1) AS limit_5_offset_1;
SELECT (SELECT k2 FROM sr_b ORDER BY k2 LIMIT 1 OFFSET 2) AS limit_1_offset_2;
SELECT (SELECT k2 FROM sr_b ORDER BY k2 LIMIT 0) AS limit_0;
SELECT name FROM sr_a WHERE k = (SELECT k2 FROM sr_b) ORDER BY name;
SELECT name FROM sr_a WHERE k = (SELECT k2 FROM sr_b ORDER BY k2 LIMIT 1) ORDER BY name;
SELECT name FROM sr_a WHERE k = (SELECT k2 FROM sr_b ORDER BY k2 LIMIT 2) ORDER BY name;
SELECT name FROM sr_a WHERE k = (SELECT k2 FROM sr_b ORDER BY k2 LIMIT 5) ORDER BY name;
SELECT name FROM sr_a WHERE k = (SELECT k2 FROM sr_b WHERE label = 'bob' LIMIT 5) ORDER BY name;
SELECT name FROM sr_a WHERE k = (SELECT k2 FROM sr_b WHERE k2 > 100 LIMIT 5) ORDER BY name;
SELECT name, (SELECT k2 FROM sr_b WHERE k2 = sr_a.k ORDER BY label LIMIT 1) AS c1 FROM sr_a ORDER BY name;
SELECT name, (SELECT k2 FROM sr_b WHERE k2 = sr_a.k ORDER BY label LIMIT 5) AS c5 FROM sr_a ORDER BY name;
SELECT name, (SELECT k2 FROM sr_b WHERE k2 = sr_a.k ORDER BY label) AS cn FROM sr_a ORDER BY name;
