/**
[env]
namespace = true
database = true

[test]
reason = "A LIMIT on a record-id range over ids of bytes, an integer and bytes, as in the report, keeps the plan and the rows in key order: EXPLAIN ANALYZE shows the Limit directly above the RecordIdScan, each emitting only the rows the LIMIT (and START) keep out of a 2000-row range. The rows an operator emits do not show what it read from storage; that the scan reads no more of a larger range is pinned by the Rust test exec::vm::tests_range_limit, which counts the keys the transaction reads."
issue = 7474

[[test.results]]
value = "[]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *] {rows: 3}
    Limit [ctx: Db] [limit: 3] {rows: 3}
        RecordIdScan [ctx: Db] [record_id: bucket_file:[b"31"]..[b"33"]] {rows: 3}

Total rows: 3''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id] {rows: 3}
    Limit [ctx: Db] [limit: 3, offset: 5] {rows: 3}
        RecordIdScan [ctx: Db] [record_id: bucket_file:[b"31"]..[b"33"]] {rows: 3}

Total rows: 3''''

[[test.results]]
value = '[bucket_file:[b"31303030", 0, b"78"], bucket_file:[b"31303031", 0, b"78"], bucket_file:[b"31303032", 0, b"78"]]'

[[test.results]]
value = '[bucket_file:[b"32393937", 0, b"78"], bucket_file:[b"32393938", 0, b"78"], bucket_file:[b"32393939", 0, b"78"]]'

[[test.results]]
value = "2000"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id] {rows: 1}
    Limit [ctx: Db] [limit: 1] {rows: 1}
        Filter [ctx: Db] [predicate: id >= (bucket_file:[b"31303530", NONE])] {rows: 1}
            TableScan [ctx: Db] [table: bucket_file, direction: Forward] {rows: 1}

Total rows: 1''''

[[test.results]]
value = '[bucket_file:[b"31303530", 0, b"78"]]'
*/

INSERT INTO bucket_file (array::map(<array> 0..2000, |$i| { id: [<bytes> <string> (1000 + $i), 0, <bytes> 'x'], host: 'example.com' })) RETURN NONE;

EXPLAIN ANALYZE SELECT * FROM bucket_file:[b"31"]..[b"33"] LIMIT 3;
EXPLAIN ANALYZE SELECT id FROM bucket_file:[b"31"]..[b"33"] START 5 LIMIT 3;
SELECT VALUE id FROM bucket_file:[b"31"]..[b"33"] LIMIT 3;
SELECT VALUE id FROM bucket_file:[b"31"]..[b"33"] START 1997 LIMIT 10;
count(SELECT id FROM bucket_file:[b"31"]..[b"33"] LIMIT 5000);

EXPLAIN ANALYZE SELECT id FROM bucket_file WHERE id >= bucket_file:[b"31303530", NONE] LIMIT 1;
SELECT VALUE id FROM bucket_file WHERE id >= bucket_file:[b"31303530", NONE] LIMIT 1;
