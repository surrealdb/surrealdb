/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "PostgreSQL join table alias resolution (u.name, o.total)"

[[test.results]]
value = "[{ name: 'Alice', total: 100 }, { name: 'Bob', total: 150 }, { name: 'Alice', total: 200 }]"
*/

SELECT u.name, o.total FROM users u JOIN orders o ON u.user_id = o.user_id ORDER BY o.total ASC;
