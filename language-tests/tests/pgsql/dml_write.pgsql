/**
[env]
namespace = true
database = true
imports = ["pgsql/_schema.surql"]

[test]
reason = "PostgreSQL INSERT DML write lowering and subsequent SELECT"

[[test.results]]
value = "[{ age: 28, id: users:4, name: 'Diana', user_id: 4 }]"

[[test.results]]
value = "[{ age: 28, name: 'Diana' }]"
*/

INSERT INTO users (id, user_id, name, age) VALUES (4, 4, 'Diana', 28);
SELECT u.name, u.age FROM users u WHERE u.user_id = 4;
