/**
[env]
namespace = true
database = true

[test]
reason = "A table a request creates holds NULL in a column that a later INSERT of the same request leaves out, as it does when the statements are sent apart"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 10i32, b: NULL, d: 7i32, id: 1 }]"

[[test.results]]
value = "[{ a: 20i32, b: NULL, d: 7i32, id: 2 }]"

[[test.results]]
value = "[{ b: NULL }]"

*/

CREATE TABLE sg (id INT PRIMARY KEY, a INT, b TEXT, d INT DEFAULT 7);

INSERT INTO sg (id, a) VALUES (1, 10);

INSERT INTO sg (id, a, b) VALUES (2, 20, NULL);

SELECT b FROM sg GROUP BY b ORDER BY b;
