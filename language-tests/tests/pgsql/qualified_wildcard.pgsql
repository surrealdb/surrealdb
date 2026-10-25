/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "The * of a relation is a select item: it selects every column of the one table of the query, qualified by the table, its alias or its schema, beside other items, and an alias after it is ignored"

[[test.results]]
value = "[{ age: 25, id: 2, name: 'Bob', user_id: 2 }, { age: 30, id: 1, name: 'Alice', user_id: 1 }, { age: 35, id: 3, name: 'Charlie', user_id: 3 }]"

[[test.results]]
value = "[{ age: 25, id: 2, name: 'Bob', next: 26, user_id: 2 }, { age: 30, id: 1, name: 'Alice', next: 31, user_id: 1 }]"

[[test.results]]
value = "[{ age: 30, id: 1, name: 'Alice', user_id: 1 }]"

[[test.results]]
value = "[{ age: 25, id: 2, name: 'Bob', user_id: 2 }]"

[[test.results]]
value = "[{ age: 35, id: 3, name: 'Charlie', user_id: 3 }]"
*/

SELECT u.* FROM users u ORDER BY u.age;

SELECT u.*, u.age + 1 AS next FROM users u WHERE u.age < 35 ORDER BY u.age;

SELECT users.* FROM users WHERE users.user_id = 1;

SELECT public.users.* FROM public.users WHERE user_id = 2;

SELECT u.* AS ignored FROM users u WHERE u.user_id = 3;
