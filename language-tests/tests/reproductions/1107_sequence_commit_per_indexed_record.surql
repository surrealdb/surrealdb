/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
issue = 1107
reason = """
An indexed write allocates a table doc-ID per record, and a table's doc-ID
allocator hands ids out of a reserved window: the window's end is committed by
the same transaction that claims the window, and ids are then served from
memory. Allocation costs one durable commit per window, not one per record, and
a restart resumes at or past every id the window could have yielded — so ids may
be skipped but never reissued.

This test pins the functional half only. The index builds and answers correctly
whichever way allocation is made durable, so nothing here would fail if the
per-record commit returned. Commit counts are asserted in Rust, where an
`ExecutionObserver` can count write-transaction completions
(`doc_id_allocation_commits_per_record`), and doc-ID uniqueness across a
mid-window restart in `doc_ids_stay_unique_across_mid_batch_restart`. Neither is
observable from SurrealQL.
"""

# DEFINE TABLE item
[[test.results]]
value = "None"

# CREATE 50 records, before any index exists
[[test.results]]
value = "[]"

# DEFINE INDEX over the 50 existing records: one doc-ID, and so one
# durable sequence commit, per record
[[test.results]]
value = "None"

# The built index answers correctly
[[test.results]]
value = "[{ count: 50 }]"

# An insert into the now-indexed table also allocates, and so also commits
[[test.results]]
value = "[]"

[[test.results]]
value = "[{ count: 51 }]"
*/

DEFINE TABLE item SCHEMALESS;
CREATE |item:1..=50| SET name = 'x' RETURN NONE;
DEFINE INDEX idx_name ON item FIELDS name;
SELECT count() FROM item WHERE name = 'x' GROUP ALL;
CREATE item:51 SET name = 'x' RETURN NONE;
SELECT count() FROM item WHERE name = 'x' GROUP ALL;
