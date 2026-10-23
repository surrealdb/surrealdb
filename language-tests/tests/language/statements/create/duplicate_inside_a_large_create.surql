/**
[env]
namespace = true
database = true
timeout = 60000

[test]
reason = """
A create's check that its record is absent is made for many creates at once, at the latest when
the statement ends. A statement of 100,000 creates whose ids repeat one fails with the duplicate
the repeated create raises, and writes nothing: a CREATE over a list of ids and an INSERT of rows
alike, with the repeat both inside the first batch of checks and past it."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Database record `dup:7` already exists"

[[test.results]]
value = "0"

[[test.results]]
error = "Database record `dup:7` already exists"

[[test.results]]
value = "0"

[[test.results]]
error = "Database record `dup:99000` already exists"

[[test.results]]
value = "0"

[[test.results]]
value = "[]"

[[test.results]]
value = "100000"
*/
DEFINE TABLE dup;
LET $ids = array::flatten([(<array> 1..=60000).map(|$i| type::record('dup', $i)), [dup:7], (<array> 60001..=100000).map(|$i| type::record('dup', $i))]);
CREATE $ids RETURN NONE;
count(SELECT VALUE id FROM dup);
INSERT INTO dup array::flatten([(<array> 1..=60000).map(|$i| { id: $i }), [{ id: 7 }], (<array> 60001..=100000).map(|$i| { id: $i })]) RETURN NONE;
count(SELECT VALUE id FROM dup);
INSERT INTO dup array::flatten([(<array> 1..=99999).map(|$i| { id: $i }), [{ id: 99000 }]]) RETURN NONE;
count(SELECT VALUE id FROM dup);
CREATE |dup:1..=100000| RETURN NONE;
count(SELECT VALUE id FROM dup);
