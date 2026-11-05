/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "PostgreSQL ~* matches without regard to case, !~ and !~* negate ~ and ~*, NULL operands give NULL, and ^ raises to a power (left-associative, double precision, NULL where there is no real result)"

[[test.results]]
value = "[{ anchored: true, case_blind: true, case_blind_negated: false, case_sensitive: false, negated: true, no_pattern: NULL, nothing: NULL }]"

[[test.results]]
value = "[{ name: 'Alice' }, { name: 'Charlie' }]"

[[test.results]]
value = "[{ name: 'Alice' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ name: 'Charlie' }]"

[[test.results]]
value = "[{ name: 'Alice' }, { name: 'Bob' }, { name: 'Charlie' }]"

[[test.results]]
value = "[{ name: 'Alice' }, { name: 'Bob' }, { name: 'Charlie' }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ name: 'Alice' }, { name: 'Bob' }, { name: 'Charlie' }]"

[[test.results]]
value = "[{ no_pattern: NULL, no_subject: NULL }]"

[[test.results]]
value = "[{ binds_tighter: 18f, left_associative: 64f, negation_first: 4f, no_real_result: NULL, nothing: NULL, overflow: NULL, power: 8f, root: true }]"

[[test.results]]
value = "[{ square: 625f }, { square: 900f }, { square: 1225f }]"

[[test.results]]
value = "[{ decimal_square: 625f, nothing: NULL, overflow: NULL, root: 5f }]"
*/

SELECT
	'abc' ~ '^a.c$' AS anchored,
	'ABC' ~* 'b' AS case_blind,
	'ABC' !~* 'b' AS case_blind_negated,
	'ABC' ~ 'b' AS case_sensitive,
	'ABC' !~ 'b' AS negated,
	NULL ~ 'b' AS nothing,
	'a' !~ NULL AS no_pattern;
SELECT name FROM users WHERE name ~ 'i' ORDER BY name;
SELECT name FROM users WHERE name ~* '^a' ORDER BY name;
SELECT name FROM users WHERE name ~ '^a' ORDER BY name;
SELECT name FROM users WHERE name !~* '^[ab]' ORDER BY name;
SELECT name FROM users WHERE name !~ '^[ab]' ORDER BY name;
SELECT name FROM users WHERE name ~* lower(name) ORDER BY name;
SELECT name FROM users WHERE name ~ lower(name) ORDER BY name;
SELECT name FROM users WHERE name !~* lower(name) ORDER BY name;
SELECT name FROM users WHERE name !~ lower(name) ORDER BY name;
SELECT name ~* NULL AS no_pattern, NULL::text ~* 'a' AS no_subject FROM users WHERE name = 'Bob';
SELECT
	2 ^ 3 AS power,
	2 ^ 3 ^ 2 AS left_associative,
	-2 ^ 2 AS negation_first,
	2 * 3 ^ 2 AS binds_tighter,
	2 ^ 0.5 > 1.41 AS root,
	0 ^ -1 AS no_real_result,
	2 ^ 5000 AS overflow,
	NULL ^ 2 AS nothing;
SELECT age ^ 2 AS square FROM users ORDER BY age;
SELECT age::numeric ^ 2 AS decimal_square, age ^ 0.5 AS root, age ^ 5000 AS overflow, age ^ NULL AS nothing FROM users WHERE name = 'Bob';
