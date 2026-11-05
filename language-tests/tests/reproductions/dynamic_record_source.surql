/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A record id in the array a FROM expression yields is the record it names: it is read, with its permissions, before the WHERE judges the row, so an id no record answers is no row and the predicate never sees it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: target:here, x: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: target:here, x: 1 }]"

[[test.results]]
value = "[{ id: target:here, x: 1 }]"
*/
DEFINE TABLE target SCHEMALESS;
CREATE target:here SET x = 1;

-- An id no record answers is dropped by the source, so a predicate that
-- would raise on it is never evaluated.
SELECT * FROM array::shuffle([target:missing]) WHERE IF true THEN THROW 'bad' ELSE false END;
-- The same, with the predicate moved into the subquery by the pushdown rule.
SELECT * FROM (SELECT * FROM array::shuffle([target:missing]))
	WHERE IF true THEN THROW 'bad' ELSE false END;
-- The same, where the array comes from a user-defined function.
DEFINE FUNCTION fn::missing() { RETURN array::shuffle([target:missing, target:gone]) };
SELECT * FROM fn::missing() WHERE IF true THEN THROW 'bad' ELSE false END;

-- A record the id does answer reaches the predicate as the record, not as
-- the id.
SELECT * FROM array::shuffle([target:here]) WHERE x = 1;
SELECT * FROM array::shuffle([target:here]) WHERE id = target:here;
