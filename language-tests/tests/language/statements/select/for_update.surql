/**
[test]
reason = "SELECT ... FOR UPDATE rejects VERSION / GROUP BY / SPLIT, LIMIT over multiple targets, and non-record targets with the same error on both executors"

[[test.results]]
value = "[{ id: person:1 }]"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE cannot be combined with VERSION"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE cannot be combined with GROUP BY or SPLIT"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE cannot be combined with GROUP BY or SPLIT"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE requires record ids as targets"

[[test.results]]
error = "Invalid query: SELECT ... FOR UPDATE cannot be combined with LIMIT over multiple targets"

*/

CREATE person:1;
SELECT * FROM person FOR UPDATE;
SELECT * FROM (SELECT * FROM person:1) FOR UPDATE;
SELECT * FROM person:1..5 FOR UPDATE;
SELECT * FROM |person:3| FOR UPDATE;
SELECT * FROM person:1 VERSION d'2024-01-01T00:00:00Z' FOR UPDATE;
SELECT count() FROM person:1 GROUP ALL FOR UPDATE;
SELECT * FROM person:1 SPLIT tags FOR UPDATE;
LET $t = type::table('person');
SELECT * FROM $t FOR UPDATE;
SELECT * FROM person:1, person:2 LIMIT 1 FOR UPDATE;
