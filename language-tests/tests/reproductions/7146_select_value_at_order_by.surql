/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Non-alias SELECT VALUE with ORDER BY must project correctly via deferred CursorDoc path"
issue = 7146

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: test:a, score: 3 }]"

[[test.results]]
value = "[{ id: test:b, score: 1 }]"

[[test.results]]
value = "[{ id: test:c, score: 2 }]"

[[test.results]]
value = "[test:b, test:c, test:a]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[3, 2, 1]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[2, 3]"
*/

DEFINE TABLE OVERWRITE test SCHEMALESS;
INSERT INTO test {id: test:a, score: 3};
INSERT INTO test {id: test:b, score: 1};
INSERT INTO test {id: test:c, score: 2};

-- Part::Doc @ must read CursorDoc.rid after deferred ORDER BY projection
SELECT VALUE @ FROM test ORDER BY score ASC;

-- Non-alias SELECT VALUE with ORDER BY ASC
SELECT VALUE score FROM test ORDER BY score ASC;

-- Non-alias SELECT VALUE with ORDER BY DESC
SELECT VALUE score FROM test ORDER BY score DESC;

-- Non-alias SELECT VALUE with ORDER BY + LIMIT
SELECT VALUE score FROM test ORDER BY score ASC LIMIT 2;

-- Non-alias SELECT VALUE with ORDER BY + START
SELECT VALUE score FROM test ORDER BY score ASC START 1;
