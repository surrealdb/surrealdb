/**
[test]
reason = "Test that rand::ulid() can generate a ULID from a specific datetime"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

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

-- Test that rand::ulid(datetime) returns a valid ULID string
LET $ulid = rand::ulid(d"2024-01-01T00:00:00Z");
type::is_string($ulid) AND string::len($ulid) == 26;

-- Test that ULIDs from different datetimes maintain temporal ordering
LET $early = rand::ulid(d"2020-01-01T00:00:00Z");
LET $late = rand::ulid(d"2025-01-01T00:00:00Z");
$early < $late;

-- Test that multiple ULIDs from the same datetime are unique (different random components)
LET $a = rand::ulid(d"2024-06-15T12:00:00Z");
LET $b = rand::ulid(d"2024-06-15T12:00:00Z");
$a != $b;

-- Test that ULIDs from the same datetime share the same timestamp prefix (first 10 chars)
LET $c = rand::ulid(d"2024-06-15T12:00:00Z");
LET $d = rand::ulid(d"2024-06-15T12:00:00Z");
string::slice($c, 0, 10) == string::slice($d, 0, 10);

-- Test using rand::ulid(datetime) in a record ID range query (dynamic expression in key)
CREATE test:[rand::ulid()] SET created = time::now(), num = 1 RETURN NONE;
SLEEP 100ms;
LET $rec = CREATE ONLY test:[rand::ulid()] SET created = time::now(), num = 2;
SLEEP 100ms;
CREATE test:[rand::ulid()] SET created = time::now(), num = 3 RETURN NONE;
SELECT VALUE num FROM test:[rand::ulid($rec.created - 50ms)]..;
