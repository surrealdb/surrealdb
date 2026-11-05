/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "The * of a derived table selects the columns of the derived table, whether the derived table selects *, the * of a table or named columns"

[[test.results]]
value = "[{ age: 25, id: 2, name: 'Bob', user_id: 2 }, { age: 30, id: 1, name: 'Alice', user_id: 1 }, { age: 35, id: 3, name: 'Charlie', user_id: 3 }]"

[[test.results]]
value = "[{ age: 30, name: 'Alice' }]"

[[test.results]]
value = "[{ age: 25, user_id: 2 }, { age: 30, user_id: 1 }, { age: 35, user_id: 3 }]"
*/

SELECT d.* FROM (SELECT * FROM users) d ORDER BY d.age;

SELECT d.name, d.age FROM (SELECT u.* FROM users u) d WHERE d.user_id = 1;

SELECT d.* FROM (SELECT user_id, age FROM users) d ORDER BY d.age;
