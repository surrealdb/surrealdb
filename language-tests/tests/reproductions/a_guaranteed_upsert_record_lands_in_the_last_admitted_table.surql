/**
[env]
imports = ["reproductions/a_guaranteed_upsert_record_lands_in_the_last_admitted_table_import.surql"]
auth = { namespace = "test", database = "test", access = "test", rid = "user:1" }

[test]
reason = """
A conditional `UPSERT` that matched nothing writes one record, once for the
whole statement. When the statement names several bare tables, that record
belongs to the last one the caller's write permission admits: a table whose
write clause denies this caller claims no record, and an admitted table later
in the list takes the place of an admitted table earlier in it.

The slot is decided per target as the statement walks them, so a denied table
neither takes the record from a table that would have had it nor holds it
against a table named after it.
"""

[[test.results]]
value = "['beta']"

[[test.results]]
value = "['allowed_second']"

[[test.results]]
value = "['allowed_first']"

[[test.results]]
value = "[]"

[[test.results]]
value = "['beta']"

[[test.results]]
value = "[]"

[[test.results]]
value = "['allowed_second']"

[[test.results]]
value = "['allowed_first']"

[[test.results]]
value = "[]"
*/

-- Every table below is empty, so no target matches and each statement owes one
-- guaranteed record. The condition keeps the statement from deferring, which is
-- what makes a bare table name the records stored there rather than a record to
-- invent.

-- Both targets are writable: the last one takes the record.
UPSERT alpha, beta SET n = 1 WHERE n = 99 RETURN VALUE record::table(id);

-- The first target is not writable: it claims nothing, and the record goes to
-- the target that is.
UPSERT denied_first, allowed_second SET n = 1 WHERE n = 99 RETURN VALUE record::table(id);

-- The last target is not writable: the record stays with the admitted target
-- named before it.
UPSERT allowed_first, denied_second SET n = 1 WHERE n = 99 RETURN VALUE record::table(id);

-- What each table actually holds afterwards.
SELECT VALUE record::table(id) FROM alpha;
SELECT VALUE record::table(id) FROM beta;
SELECT VALUE record::table(id) FROM denied_first;
SELECT VALUE record::table(id) FROM allowed_second;
SELECT VALUE record::table(id) FROM allowed_first;
SELECT VALUE record::table(id) FROM denied_second;
