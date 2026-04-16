/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "SELECT VALUE expr AS alias ORDER BY alias must not evaluate expr twice"
issue = 7146

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:a, score: 30 }]"

[[test.results]]
value = "[{ id: test:b, score: 10 }]"

[[test.results]]
value = "[{ id: test:c, score: 20 }]"

[[test.results]]
value = "[20, 40, 60]"

[[test.results]]
value = "[60, 40, 20]"

[[test.results]]
value = "[20, 40]"

[[test.results]]
value = "[40, 60]"
*/

DEFINE TABLE OVERWRITE test SCHEMALESS;
INSERT INTO test {id: test:a, score: 30};
INSERT INTO test {id: test:b, score: 10};
INSERT INTO test {id: test:c, score: 20};

-- Alias-based ORDER BY: the aliased value must be in sorted order
SELECT VALUE score * 2 AS doubled FROM test ORDER BY doubled ASC;

-- Alias-based ORDER BY DESC
SELECT VALUE score * 2 AS doubled FROM test ORDER BY doubled DESC;

-- Alias-based ORDER BY with LIMIT
SELECT VALUE score * 2 AS doubled FROM test ORDER BY doubled ASC LIMIT 2;

-- Alias-based ORDER BY with START
SELECT VALUE score * 2 AS doubled FROM test ORDER BY doubled ASC START 1;
