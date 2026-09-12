/**
[test]
reason = "EXPLAIN renders stable plans for four shapes: recursive idiom (`.{..3}`), destructuring projection (`{field, nested:{...}}`), ORDER+LIMIT+START, and FETCH on a record link."

[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[[test.results]]
value = "NONE"

[[test.results]]
value = '''
'Project [ctx: Db]
  field.recurse: Recurse [ctx: Db] [depth: 1..3, instruction: default]
    RecordIdScan [ctx: Db] [record_id: node:a]
''''

[[test.results]]
value = '''
'Project [ctx: Db]
    RecordIdScan [ctx: Db] [record_id: shape:1]
''''

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: *]
    Limit [ctx: Db] [limit: 2, offset: 1]
        SortTopKByKey [ctx: Db] [sort_keys: label DESC, limit: 3]
            TableScan [ctx: Db] [table: node, direction: Forward, topk_pushdown: yes]
''''

[[test.results]]
value = '''
'Fetch [ctx: Db] [fields: child]
    SelectProject [ctx: Db] [projections: label, child]
        RecordIdScan [ctx: Db] [record_id: node:a]
''''

*/

{
    CREATE node:a SET label = 'A', child = node:b;
    CREATE node:b SET label = 'B', child = node:c;
    CREATE node:c SET label = 'C';
    CREATE shape:1 SET
        outer = { a: 'x', b: 'y', nested: { z: 1, w: 2 } },
        meta = { created: 'now', tag: 'first' };
    RETURN NONE;
};

EXPLAIN SELECT label, child.{..3}.label FROM node:a;
EXPLAIN SELECT outer.{a, b, nested.{z}} FROM shape:1;
EXPLAIN SELECT * FROM node ORDER BY label DESC LIMIT 2 START 1;
EXPLAIN SELECT label, child FROM node:a FETCH child;
