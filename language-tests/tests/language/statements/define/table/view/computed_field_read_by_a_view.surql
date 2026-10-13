/**
[env]
namespace = true
database = true

[test]
reason = """
A `COMPUTED` field is stripped before the record is stored, so a foreign view
that groups by it, projects it or filters on it is handed the derived value
rather than reading it back out of the record. The view is the only thing that
observes the write here: the table has no event, no live query and no
changefeed.

The aggregated view pins both images. An update that moves a record from one
group to another reads the old group off the pre-write record, and a delete reads
the group it leaves off the same image; without the derived pre-image the count
would be taken from the wrong group. The filtered view pins the condition, which
is judged on the post-write record.

A statement's own return value carries the computed field as it does on a table
with no view.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ d: 'q', id: grouped:['q'], n: 2 }, { d: 'r', id: grouped:['r'], n: 1 }]"

[[test.results]]
value = "[{ dd: 'q', id: copied:1, src: 'q' }, { dd: 'q', id: copied:2, src: 'q' }, { dd: 'r', id: copied:3, src: 'r' }]"

[[test.results]]
value = "[{ d: 'q', id: kept:1 }, { d: 'q', id: kept:2 }]"

[[test.results]]
value = "[{ d: 'r', id: b:1, src: 'r' }]"

[[test.results]]
value = "[{ d: 'q', id: grouped:['q'], n: 1 }, { d: 'r', id: grouped:['r'], n: 2 }]"

[[test.results]]
value = "[{ dd: 'r', id: copied:1, src: 'r' }, { dd: 'q', id: copied:2, src: 'q' }, { dd: 'r', id: copied:3, src: 'r' }]"

[[test.results]]
value = "[{ d: 'q', id: kept:2 }]"

[[test.results]]
value = "[{ d: 'q', id: b:2, src: 'q' }]"

[[test.results]]
value = "[{ d: 'r', id: grouped:['r'], n: 2 }]"

[[test.results]]
value = "[{ dd: 'r', id: copied:1, src: 'r' }, { dd: 'r', id: copied:3, src: 'r' }]"

[[test.results]]
value = "[]"
*/

DEFINE TABLE b SCHEMALESS;
DEFINE FIELD src ON b TYPE option<string>;
DEFINE FIELD d ON b COMPUTED src;
DEFINE TABLE grouped AS SELECT count() AS n, d FROM b GROUP BY d;
DEFINE TABLE copied AS SELECT d AS dd, src FROM b;
DEFINE TABLE kept AS SELECT d FROM b WHERE d = 'q';

CREATE b:1 SET src = 'q' RETURN NONE;
CREATE b:2 SET src = 'q' RETURN NONE;
CREATE b:3 SET src = 'r' RETURN NONE;
SELECT * FROM grouped ORDER BY d;
SELECT * FROM copied ORDER BY id;
SELECT * FROM kept ORDER BY id;

-- `b:1` leaves the `q` group for the `r` group, and the filtered view drops it.
UPDATE b:1 SET src = 'r';
SELECT * FROM grouped ORDER BY d;
SELECT * FROM copied ORDER BY id;
SELECT * FROM kept ORDER BY id;

-- The last record of the `q` group goes, and so does the group.
DELETE b:2 RETURN BEFORE;
SELECT * FROM grouped ORDER BY d;
SELECT * FROM copied ORDER BY id;
SELECT * FROM kept ORDER BY id;
