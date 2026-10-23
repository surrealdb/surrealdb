/**
[test]
reason = """
A record's incoming reference keys are scanned so each referring field's ON
DELETE strategy can be applied, and every strategy but IGNORE writes to the
same transaction that scan reads from. The strategies run with no scan open:
the whole range is drained and the cursor closed before any strategy is
applied.

A backend's cursor may hold an iterator borrowed from the transaction itself,
and writing through the transaction while one is alive is outside what that
iterator promises. The UPSERT here rolls back to a savepoint from its second run
onwards, which an engine may refuse while a cursor is alive rather than let the
iterator read freed state, so a strategy reached from inside an open scan would
take down the whole statement.

The tally counts strategy runs, so it holds one per reference key. The 600
references that follow the first two span more than one cursor batch, so the
second and third deletes drain the range across batches.
"""

# 0, 1, 2
[[test.results]]
value = "NONE"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"

# 3, 4: two references — the strategy rolls back with no scan open
[[test.results]]
value = "[]"
[[test.results]]
value = "2"

# 5, 6: 600 more, spanning cursor batches
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"

# 7, 8: every key ran the strategy exactly once
[[test.results]]
value = "[]"
[[test.results]]
value = "602"

# 9, 10, 11: CASCADE over references spanning cursor batches
[[test.results]]
value = "NONE"
[[test.results]]
value = "[]"
[[test.results]]
value = "[]"

# 12, 13
[[test.results]]
value = "[]"
[[test.results]]
value = "[{ count: 0 }]"

*/

-- 0, 1, 2
DEFINE FIELD author ON comment TYPE record<person> REFERENCE ON DELETE THEN (UPSERT tally:runs SET n += 1);
CREATE person:one RETURN NONE;
CREATE comment:1, comment:2 SET author = person:one RETURN NONE;

-- 3, 4
DELETE person:one;
tally:runs.n;

-- 5, 6
CREATE person:two RETURN NONE;
CREATE |comment:3..=602| SET author = person:two RETURN NONE;

-- 7, 8
DELETE person:two;
tally:runs.n;

-- 9, 10, 11
DEFINE FIELD owner ON receipt TYPE record<account> REFERENCE ON DELETE CASCADE;
CREATE account:one RETURN NONE;
CREATE |receipt:1..=600| SET owner = account:one RETURN NONE;

-- 12, 13
DELETE account:one;
SELECT count() FROM receipt GROUP ALL;
