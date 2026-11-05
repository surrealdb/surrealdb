/**
[env]
imports = ["upgrade/value/3.02.04_record_id_datetime_import.surql"]

[test]
upgrade = true
# The import writes a Datetime inside an array or object record id, which
# 3.0.0's parser rejects; 3.2.4 and 3.3.0 accept it.
introduced = "3.2.4"
reason = """
A compound record id holding a Datetime is part of the record's key, which no
operator step rewrites. A lookup by id on the new binary spells the Datetime as
the old writer did, as RFC 3339 text, or the record would be unreachable by its
own id while a table scan still returns it. An index stamped at the current
format version spells its indexed Datetime values differently; record keys keep
the one spelling."""

[[test.results]]
value = "[0, 1, 2, 3]"

[[test.results]]
value = "[0]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[0, 1, 2]"

[[test.results]]
value = "[0]"

*/
-- A table scan reads every record whatever its key spells.
SELECT VALUE n FROM reading ORDER BY n;

-- A whole-second Datetime inside an array id.
SELECT VALUE n FROM reading:['a', d'2026-08-28T09:00:00Z'];

-- Fractional seconds at millisecond and microsecond precision.
SELECT VALUE n FROM reading:['a', d'2026-08-28T09:00:00.152Z'];
SELECT VALUE n FROM reading:['a', d'2026-08-28T09:00:00.152300Z'];

-- A Datetime inside an object id.
SELECT VALUE n FROM reading:{ at: d'2026-08-28T09:00:01Z', k: 'a' };

-- A range over the leading element scans the records and decodes their keys.
SELECT VALUE n FROM reading:['a']..['b'] ORDER BY n;

-- A range bounded by a Datetime spells its bounds as the keys were written.
SELECT VALUE n FROM reading:['a', d'2026-08-28T09:00:00Z']..=['a', d'2026-08-28T09:00:00Z'];
