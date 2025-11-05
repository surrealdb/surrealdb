/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[2, 3]"

*/
CREATE test:[rand::uuid::v7()] SET created = time::now(), num = 1 RETURN NONE;
SLEEP 100ms;
LET $rec = CREATE ONLY test:[rand::uuid::v7()] SET created = time::now(), num = 2;
SLEEP 100ms;
CREATE test:[rand::uuid::v7()] SET created = time::now(), num = 3 RETURN NONE;
SELECT VALUE num FROM test:[rand::uuid::v7($rec.created - 50ms)]..;
