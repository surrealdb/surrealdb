/**
[env]
namespace = true
database = true
imports = ["pgsql/_omitted_column.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "users:alice" }

[test]
reason = "A record user is told nothing of the columns a table declares, so an INSERT of its own leaves them out as it names them: writing NULL to the columns it did not name would tell it which columns exist"

[[test.results]]
value = "[{ a: 10, b: NONE, d: 7, id: 1 }]"

[[test.results]]
value = "[{ a: 10, d: 7, id: 1 }]"
*/

INSERT INTO omit_t (id, a) VALUES (1, 10) RETURNING id, a, b, d;

SELECT * FROM omit_t ORDER BY id;
