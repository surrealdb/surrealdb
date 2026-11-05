/**
[env]
namespace = true
database = true

[test]
reason = """
The WHERE condition of an updating statement is checked per record BEFORE the
data clause is evaluated, so a data clause with side effects runs only for
records the condition accepts. This also makes the full-scan path agree with
the index-backed path, where rejected records never enter the write pipeline at
all: the number of side effects equals the number of matching records
regardless of which physical plan runs the statement. The `fs` and `idx` tables
run the same statement with and without an index on the condition field and
must produce the same side-effect count.
"""

[[test.results]]
value = "[{ id: fs:1, n: 1 }]"

[[test.results]]
value = "[{ id: fs:2, n: 2 }]"

[[test.results]]
value = "[{ id: fs:3, n: 3 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "0"

[[test.results]]
value = "0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: idx:1, n: 1 }]"

[[test.results]]
value = "[{ id: idx:2, n: 2 }]"

[[test.results]]
value = "[{ id: idx:3, n: 3 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "1"

[[test.results]]
value = "1"
*/

CREATE fs:1 SET n = 1;
CREATE fs:2 SET n = 2;
CREATE fs:3 SET n = 3;
-- Pre-defined because the assertion is that it stays EMPTY: the data clause
-- must never run, so nothing else would bring the table into existence.
DEFINE TABLE log_none;
-- A condition that rejects every record: the data clause must not run at all.
UPDATE fs SET spawn = (CREATE log_none).id WHERE false;
RETURN array::len((SELECT id FROM log_none));
RETURN array::len((SELECT id FROM fs WHERE spawn != NONE));

-- The same statement over an indexed and an unindexed table must create the
-- same number of side-effect records: one per matching record.
DEFINE INDEX n_idx ON idx FIELDS n;
CREATE idx:1 SET n = 1;
CREATE idx:2 SET n = 2;
CREATE idx:3 SET n = 3;
UPDATE fs SET spawn = (CREATE log_fs).id WHERE n = 2 RETURN NONE;
UPDATE idx SET spawn = (CREATE log_idx).id WHERE n = 2 RETURN NONE;
RETURN array::len((SELECT id FROM log_fs));
RETURN array::len((SELECT id FROM log_idx));
