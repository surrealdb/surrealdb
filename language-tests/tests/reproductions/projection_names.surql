/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = """An unaliased projection whose derived name would be spelled from a nested \
plan — a statement in scalar position — is keyed by the SurrealQL text of the \
expression as written, which is the key the legacy engine gives it. A path that starts \
with such a statement is keyed by the path's text; the legacy engine drops that field \
from the row, so the streaming engine is pinned alone."""

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "[{ id: b:1 }]"

[[test.results]]
value = "[{ \"SELECT * FROM b\": [{ id: b:1 }] }]"

[[test.results]]
value = "[{ \"IF true { 1 } ELSE { 2 }\": 1 }]"

[[test.results]]
value = "[{ \"{ RETURN 1 }\": 1 }]"

[[test.results]]
value = "[{ \"RETURN 1\": 1 }]"

[[test.results]]
value = "[{ \"(SELECT VALUE id FROM b)[0]\": b:1 }]"

[[test.results]]
value = "[{ \"(SELECT * FROM b).id\": [b:1] }]"

[[test.results]]
value = "[{ \"IF true THEN 1 ELSE 2 END\": 1 }]"

[[test.results]]
value = "[{ \"(SELECT * FROM ONLY b LIMIT 1).id\": b:1 }]"

[[test.results]]
value = "[{ sub: [{ id: b:1 }] }]"

[[test.results]]
value = "[[{ id: b:1 }]]"

[[test.results]]
value = "[{ \"[(SELECT * FROM b)]\": [[{ id: b:1 }]] }]"
*/

CREATE a:1; CREATE b:1;
SELECT (SELECT * FROM b) FROM a;
SELECT IF true { 1 } ELSE { 2 } FROM a;
SELECT { RETURN 1 } FROM a;
SELECT (RETURN 1) FROM a;
SELECT (SELECT VALUE id FROM b)[0] FROM a;
SELECT (SELECT * FROM b).id FROM a;
SELECT IF true THEN 1 ELSE 2 END FROM a;
SELECT (SELECT * FROM ONLY b LIMIT 1).id FROM a;
SELECT (SELECT * FROM b) AS sub FROM a;
SELECT VALUE (SELECT * FROM b) FROM a;
SELECT [(SELECT * FROM b)] FROM a;
