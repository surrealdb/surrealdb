/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "ORDER BY a field that is not in the SELECT list is allowed. The engine sorts by the non-projected field and returns only the selected columns."

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: t:1, name: 'alice' }, { id: t:2, name: 'bob' }, { id: t:3, name: 'charlie' }]"

[[test.results]]
value = "[{ id: t:3, name: 'charlie' }, { id: t:2, name: 'bob' }, { id: t:1, name: 'alice' }]"

*/

-- Setup: records with `age` NOT in SELECT
{
	CREATE t:1 SET name = "alice", age = 30;
	CREATE t:2 SET name = "bob",   age = 20;
	CREATE t:3 SET name = "charlie", age = 25;
	RETURN "OK";
};

-- ORDER BY age ASC but only SELECT id and name (age not in output)
SELECT id, name FROM t ORDER BY age ASC;

-- ORDER BY age DESC
SELECT id, name FROM t ORDER BY age DESC;
