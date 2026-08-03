/**
[env]
imports = ["language/planner/record_idiom_fold_auth_import.surql"]
planner-strategy = ["all-ro"]

auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = "The plan-time record-idiom fold requires permission checks to be provably disabled: under a record-user session the fold must not run (the plan-time walk applies no table- or field-level SELECT permissions), so the idiom stays unfolded and the source plans as a table scan even though the table grants FULL select"

[[test.results]]
value = "{ attributes: { projections: 'id' }, children: [{ attributes: { direction: 'Forward', pre_decode_filter: 'no (unsupported predicate)', predicate: 'due <= (cfg:one).cutoff', table: 'item' }, context: 'Db', operator: 'TableScan' }], context: 'Db', operator: 'SelectProject' }"

[[test.results]]
value = "[{ id: item:a }]"
*/

SELECT id FROM item WHERE due <= (cfg:one).cutoff EXPLAIN;
SELECT id FROM item WHERE due <= (cfg:one).cutoff;
