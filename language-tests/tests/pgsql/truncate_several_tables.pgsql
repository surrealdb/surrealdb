/**
[env]
namespace = true
database = true

[test]
reason = "TRUNCATE empties every table it lists"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, v: 1i32 }]"

[[test.results]]
value = "[{ id: 1, v: 1i32 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ n: 0 }]"

[[test.results]]
value = "[{ n: 0 }]"
*/

CREATE TABLE ta (id int primary key, v int);
CREATE TABLE tb (id int primary key, v int);
INSERT INTO ta (id, v) VALUES (1, 1);
INSERT INTO tb (id, v) VALUES (1, 1);
TRUNCATE TABLE ta, tb;
SELECT count(*) AS n FROM ta;
SELECT count(*) AS n FROM tb;
