/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Pre-decode filter for CONTAINS / CONTAINSNOT on strings and arrays, including nested paths. EXPLAIN asserts pre_decode_filter: yes."

[[test.results]]
value = "{ id: pcf:1, outer: { list: [1, 2, 3] }, s: 'foo_omicron_bar' }"

[[test.results]]
value = "{ id: pcf:2, outer: { list: [9] }, s: 'no_match' }"

[[test.results]]
value = "[{ id: pcf:1, outer: { list: [1, 2, 3] }, s: 'foo_omicron_bar' }]"

[[test.results]]
value = "[{ id: pcf:2, outer: { list: [9] }, s: 'no_match' }]"

[[test.results]]
value = "[{ id: pcf:1, outer: { list: [1, 2, 3] }, s: 'foo_omicron_bar' }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *]\n    TableScan [ctx: Db] [table: pcf, direction: Forward, predicate: s CONTAINS 'omicron', pre_decode_filter: yes]\n"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *]\n    TableScan [ctx: Db] [table: pcf, direction: Forward, predicate: s CONTAINSNOT 'omicron', pre_decode_filter: yes]\n"'''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    TableScan [ctx: Db] [table: pcf, direction: Forward, predicate: outer.list CONTAINS 2, pre_decode_filter: yes]\n''''

*/

CREATE ONLY pcf:1 SET s = 'foo_omicron_bar', outer = { list: [1, 2, 3] };
CREATE ONLY pcf:2 SET s = 'no_match', outer = { list: [9] };

SELECT * FROM pcf WHERE s CONTAINS 'omicron';
SELECT * FROM pcf WHERE s CONTAINSNOT 'omicron';
SELECT * FROM pcf WHERE outer.list CONTAINS 2;

EXPLAIN SELECT * FROM pcf WITH NOINDEX WHERE s CONTAINS 'omicron';
EXPLAIN SELECT * FROM pcf WITH NOINDEX WHERE s CONTAINSNOT 'omicron';
EXPLAIN SELECT * FROM pcf WITH NOINDEX WHERE outer.list CONTAINS 2;
