/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "A subquery's alias for a table frees the table's own name to mean the same table of the enclosing query, as in PostgreSQL"

[[test.results]]
value = "[{ name: 'Alice' }]"

[[test.results]]
value = "[{ at_least_as_old: 2, name: 'Alice' }, { at_least_as_old: 3, name: 'Bob' }, { at_least_as_old: 1, name: 'Charlie' }]"
*/

SELECT name FROM users WHERE EXISTS (SELECT 1 FROM users u2 WHERE users.name = 'Alice' AND u2.user_id = users.user_id) ORDER BY name;

SELECT name, (SELECT count(*) FROM users u2 WHERE u2.age >= users.age) AS at_least_as_old FROM users ORDER BY name;
