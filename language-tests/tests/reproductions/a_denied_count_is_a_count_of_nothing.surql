/**
[env]
imports = ["reproductions/a_denied_count_is_a_count_of_nothing_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "user:1" }

[test]
reason = """
A count reads keys rather than records, so the plan that answers it resolves
the table's `SELECT` permission itself. A caller who may read no record of a
table counts none of them, and `GROUP ALL` reports that count as a row, the
same row it reports for an empty table — which is all the caller is entitled
to know, and all an empty table would have told them anyway.

The two ways of denying every row are the point of the pair: a table-level
`PERMISSIONS NONE`, which the count plans resolve once, and a per-record
predicate that no record satisfies, which they resolve per record. They must
answer alike, and alike in each shape a different plan answers: a whole table,
a range of record ids, an indexed predicate, and a projection that carries a
second column so no count shortcut applies at all.

`shown` is the control: the same statements over a readable record report it.
"""

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0, x: 1 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ count: 0, x: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1 }]"

[[test.results]]
value = "[{ count: 1, x: 1 }]"
*/

-- A table the caller may not read at all.
SELECT count() FROM denied GROUP ALL;
SELECT count() FROM denied:1..9 GROUP ALL;
SELECT count() FROM denied WHERE k = 1 GROUP ALL;
SELECT count(), 1 AS x FROM denied GROUP ALL;

-- A table whose records the caller may read none of.
SELECT count() FROM guarded GROUP ALL;
SELECT count() FROM guarded:1..9 GROUP ALL;
SELECT count() FROM guarded WHERE k = 1 GROUP ALL;
SELECT count(), 1 AS x FROM guarded GROUP ALL;

-- Control: a record the caller may read is counted.
SELECT count() FROM shown GROUP ALL;
SELECT count() FROM shown WHERE k = 1 GROUP ALL;
SELECT count(), 1 AS x FROM shown GROUP ALL;
