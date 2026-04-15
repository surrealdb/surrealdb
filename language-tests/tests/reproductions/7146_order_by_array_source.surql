/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ORDER BY must not be ignored when SELECT VALUE is used with FROM [array_of_record_ids]"
issue = 7146

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ active: true, id: ct:a, v: 3 }]"

[[test.results]]
value = "[{ active: true, id: ct:m, v: 2 }]"

[[test.results]]
value = "[{ active: true, id: ct:z, v: 1 }]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[3, 2, 1]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[2, 3]"
*/

DEFINE TABLE OVERWRITE ct SCHEMALESS;
INSERT INTO ct {id: ct:a, v: 3, active: true};
INSERT INTO ct {id: ct:m, v: 2, active: true};
INSERT INTO ct {id: ct:z, v: 1, active: true};

-- Core reproduction: ORDER BY v ASC with WHERE
SELECT VALUE v FROM [ct:a, ct:m, ct:z] WHERE active ORDER BY v ASC;

-- ORDER BY DESC variant
SELECT VALUE v FROM [ct:a, ct:m, ct:z] WHERE active ORDER BY v DESC;

-- ORDER BY without WHERE
SELECT VALUE v FROM [ct:a, ct:m, ct:z] ORDER BY v ASC;

-- ORDER BY with LIMIT with alias
SELECT VALUE v AS x FROM [ct:a, ct:m, ct:z] ORDER BY x ASC LIMIT 2;

-- ORDER BY with WHERE using source field, alias in ORDER BY
SELECT VALUE v AS x FROM [ct:a, ct:m, ct:z] WHERE v > 1 ORDER BY x ASC;
