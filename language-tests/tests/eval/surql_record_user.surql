/**
[env]
imports = ["eval/_permission_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:bob" }

[env.capabilities]
allow-eval-query = ["record", "system"]

[test]
reason = """
A record user allowed eval: a SELECT permission that calls eval::surql
filters the rows it is judged for, a function body calls it, it reads the
caller's own $auth, and a write it attempts runs under the caller's
permissions."""

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "42"

[[test.results]]
value = "user:bob"

[[test.results]]
value = "[[user:bob, 2], [user:bob, 3]]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[2, 3]"
*/
SELECT VALUE n FROM item ORDER BY n;
RETURN fn::double(21);
RETURN eval::surql("RETURN $auth.id");
SELECT VALUE eval::surql("RETURN [$auth.id, $n]", { n: n }) FROM item ORDER BY n;
RETURN eval::surql("CREATE item:9 SET n = 9");
SELECT VALUE n FROM item ORDER BY n;
