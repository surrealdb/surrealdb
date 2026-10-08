/**
[env]
namespace = true
database = true

[test]
reason = "Deep nested-path predicates (a.b.c.d.e = value) descend through several Object levels. The recursive descent borrows each level's bytes from its parent's stack frame — no intermediate Vec<u8> copy. Behaviour must stay correct for both leaf hit and intermediate-missing cases; EXPLAIN shows the predicate as a filter over the table scan."

[[test.results]]
value = "{ a: { b: { c: { d: { e: 42 } } } }, id: pfdeep:1 }"

[[test.results]]
value = "{ a: { b: { c: { d: { e: 7 } } } }, id: pfdeep:2 }"

[[test.results]]
value = "[{ id: pfdeep:1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pfdeep:1 }]"

[[test.results]]
value = '''
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: pfdeep, direction: Forward, predicate: a.b.c.d.e = 42, pre_decode_filter: yes]
''''

*/

CREATE ONLY pfdeep:1 SET a = { b: { c: { d: { e: 42 } } } };
CREATE ONLY pfdeep:2 SET a = { b: { c: { d: { e: 7 } } } };

SELECT id FROM pfdeep WHERE a.b.c.d.e = 42;
SELECT id FROM pfdeep WHERE a.missing.c.d.e = 42;
SELECT id FROM pfdeep WHERE a.b.c.d.e = 42 AND id = pfdeep:1;

EXPLAIN SELECT id FROM pfdeep WITH NOINDEX WHERE a.b.c.d.e = 42;
