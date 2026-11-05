/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "PostgreSQL schema qualification public.users with alias"

[[test.results]]
value = "[{ age: 25, name: 'Bob' }, { age: 30, name: 'Alice' }, { age: 35, name: 'Charlie' }]"
*/

SELECT u.name, u.age FROM public.users u ORDER BY u.age ASC;
