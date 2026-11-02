/**
[test]
requires = ["scripting"]
reason = "A write loads its table's events when it starts: an event an embedded script defines while it runs fires for none of its rows, and for every row of the next write."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }, { id: t:3 }]"

[[test.results]]
value = "[{ id: t:1 }, { id: t:2 }, { id: t:3 }]"

[[test.results]]
error = "The table 'log' does not exist"

[[test.results]]
value = "[{ b: 1, id: t:1 }, { b: 1, id: t:2 }, { b: 1, id: t:3 }]"

[[test.results]]
value = "[{ r: t:1 }, { r: t:2 }, { r: t:3 }]"
*/
DEFINE FUNCTION fn::x() { RETURN function() { return surrealdb.query("DEFINE EVENT OVERWRITE e ON t THEN { CREATE log SET r = $after.id }") } };
CREATE t:1, t:2, t:3;
UPDATE t SET a = fn::x();
SELECT r FROM log ORDER BY r;
UPDATE t SET b = 1;
SELECT r FROM log ORDER BY r;
