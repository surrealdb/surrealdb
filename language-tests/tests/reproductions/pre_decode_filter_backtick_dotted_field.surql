/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Pre-decode filter must treat backtick-quoted dotted field names as a single flat object key (same as SimpleBinaryOp runtime), not as nested paths."

[[test.results]]
value = "[{ \"a.b\": 5, id: pfb:1 }]"

[[test.results]]
value = "[{ id: pfb:1 }]"

[[test.results]]
value = """
'SelectProject [ctx: Db] [projections: id]
    TableScan [ctx: Db] [table: pfb, direction: Forward, predicate: a.b = 5, pre_decode_filter: yes]
'"""
*/

CREATE pfb:1 SET `a.b` = 5;

SELECT id FROM pfb WHERE `a.b` = 5;

EXPLAIN SELECT id FROM pfb WHERE `a.b` = 5;
