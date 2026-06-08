/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "EXPLAIN still reports pre_decode_filter: yes for streaming-eligible CONTAINS / CONTAINSANY (LeafStreaming is invisible to EXPLAIN)."

[[test.results]]
value = "{ id: psft:1, s: 'hello', tags: ['x', 'y'] }"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: psft:1, s: 'hello', tags: ['x', 'y'] }]"

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: psft, direction: Forward, predicate: s CONTAINS 'needle', pre_decode_filter: yes]
"'''

[[test.results]]
value = '''"SelectProject [ctx: Db] [projections: *]
    TableScan [ctx: Db] [table: psft, direction: Forward, predicate: tags CONTAINSANY ['a', 'b'], pre_decode_filter: yes]
"'''

*/

CREATE ONLY psft:1 SET s = 'hello', tags = ['x', 'y'];

SELECT * FROM psft WHERE s CONTAINS 'needle';
SELECT * FROM psft WHERE s CONTAINS 'ell';

EXPLAIN SELECT * FROM psft WITH NOINDEX WHERE s CONTAINS 'needle';
EXPLAIN SELECT * FROM psft WITH NOINDEX WHERE tags CONTAINSANY ['a', 'b'];
