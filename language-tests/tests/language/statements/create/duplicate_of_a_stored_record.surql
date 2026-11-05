/**
[env]
namespace = true
database = true
timeout = 60000

[test]
reason = """
A create whose record is already stored fails with that record's duplicate when its check is made
for many creates at once: the first failing create in the statement's order is the one reported,
the statement writes nothing, and an earlier failure is reported over a later row's other error."""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database record `e:900` already exists"

[[test.results]]
value = "999"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database record `e:99999` already exists"

[[test.results]]
value = "1000"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Database record `a:5` already exists"

[[test.results]]
error = "Found 40 for field `n`, with record `a:40`, but field must conform to: $value < 40"

[[test.results]]
value = "1"
*/
DEFINE TABLE e;
CREATE |e:1..1000| RETURN NONE;
CREATE |e:2000..2100|, |e:900..2000| RETURN NONE;
count(SELECT VALUE id FROM e);
CREATE e:99999 RETURN NONE;
CREATE |e:5000..=105000| RETURN NONE;
count(SELECT VALUE id FROM e);
DEFINE FIELD n ON a ASSERT $value < 40;
CREATE a:5 SET n = 5 RETURN NONE;
INSERT INTO a (<array> 1..=60).map(|$i| { id: $i, n: $i }) RETURN NONE;
INSERT INTO a (<array> 6..=60).map(|$i| { id: $i, n: $i }) RETURN NONE;
count(SELECT VALUE id FROM a);
