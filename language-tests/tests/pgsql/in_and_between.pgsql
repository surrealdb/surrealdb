/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "PostgreSQL BETWEEN and IN operators"

[[test.results]]
value = "[{ age: 25, name: 'Bob' }, { age: 30, name: 'Alice' }]"

[[test.results]]
value = "[{ name: 'Alice' }, { name: 'Charlie' }]"
*/

SELECT u.name, u.age FROM users u WHERE u.age BETWEEN 25 AND 30 ORDER BY u.age ASC;
SELECT u.name FROM users u WHERE u.user_id IN (1, 3) ORDER BY u.name ASC;
