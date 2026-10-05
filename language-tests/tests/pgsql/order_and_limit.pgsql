/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "PostgreSQL ORDER BY DESC with LIMIT"

[[test.results]]
value = "[{ age: 35, name: 'Charlie' }, { age: 30, name: 'Alice' }]"
*/

SELECT u.name, u.age FROM users u ORDER BY u.age DESC LIMIT 2;
