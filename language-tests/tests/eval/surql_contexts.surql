/**
[env.capabilities]
allow-eval-query = ["system"]

[test]
reason = """
eval::surql runs where any expression runs: a function body, a table's
PERMISSIONS clause (bypassed for the owner), a field's VALUE clause, an
event's THEN clause, a closure and a closure handed to a builtin, and a
projection over many rows with bindings read per row. The nested query is
isolated: `$this` is not the caller's, while the session's own values are."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "42"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:1, n: 1 }]"

[[test.results]]
value = "[{ id: item:2, n: 2 }]"

[[test.results]]
value = "[{ id: item:3, n: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: item:2, n: 5, twice: 10 }]"

[[test.results]]
value = "[item:2]"

[[test.results]]
value = "[{ n: 1, twice: NONE }, { n: 3, twice: NONE }, { n: 5, twice: 10 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "42"

[[test.results]]
value = "[10, 20, 30]"

[[test.results]]
value = "[101, 103, 105]"

[[test.results]]
value = "[1, 3, 5]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'test'"
*/
DEFINE FUNCTION fn::double($x: int) { RETURN eval::surql("RETURN $v * 2", { v: $x }) };
RETURN fn::double(21);
DEFINE TABLE item SCHEMALESS PERMISSIONS FOR select WHERE eval::surql("RETURN $n > 1", { n: n }), FOR create, update, delete FULL;
CREATE item:1 SET n = 1;
CREATE item:2 SET n = 2;
CREATE item:3 SET n = 3;
DEFINE FIELD twice ON item VALUE eval::surql("RETURN $v * 2", { v: n });
DEFINE EVENT logged ON item WHEN $event = "UPDATE" THEN eval::surql("CREATE log SET of = $id", { id: $after.id });
UPDATE item:2 SET n = 5;
SELECT VALUE of FROM log;
SELECT n, twice FROM item ORDER BY n;
LET $f = |$x: int| eval::surql("RETURN $y + 1", { y: $x });
RETURN $f(41);
RETURN [1, 2, 3].map(|$x| eval::surql("RETURN $y * 10", { y: $x }));
SELECT VALUE eval::surql("RETURN $n + 100", { n: n }) FROM item ORDER BY n;
SELECT VALUE eval::surql("SELECT VALUE n FROM ONLY $r", { r: id }) FROM item ORDER BY n;
RETURN eval::surql("RETURN $this");
RETURN eval::surql("RETURN $auth");
RETURN eval::surql("RETURN $session.ns");
