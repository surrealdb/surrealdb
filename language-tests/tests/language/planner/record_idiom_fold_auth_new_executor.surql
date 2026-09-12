/**
[env]
imports = ["language/planner/record_idiom_fold_auth_import.surql"]
planner-strategy = ["all-ro"]

auth = { namespace = "test", database = "test", access = "user", rid = "user:alice" }

[test]
reason = "The plan-time record-idiom fold requires permission checks to be provably disabled: under a record-user session the fold must not run (the plan-time walk applies no table- or field-level SELECT permissions), so the idiom stays unfolded and the source plans as a table scan even though the table grants FULL select"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: item, direction: Forward, predicate: due <= (cfg:one).cutoff, pre_decode_filter: no (unsupported predicate)]
''''

[[test.results]]
value = "[{ id: item:a }]"
*/

EXPLAIN SELECT id FROM item WHERE due <= (cfg:one).cutoff;
SELECT id FROM item WHERE due <= (cfg:one).cutoff;
