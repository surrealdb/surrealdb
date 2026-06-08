/**
[env]
namespace = true
database = true
planner-strategy = ["all-ro"]

[test]
reason = "Pre-decode filter for INSIDE / NOTINSIDE on literal arrays (hashset path), mixed numbers, missing field, range RHS, nested object element, and large IN list. EXPLAIN asserts pre_decode_filter: yes."

[[test.results]]
value = "{ id: pin:1, s: 'hello', v: 10 }"

[[test.results]]
value = "{ id: pin:2, v: 20 }"

[[test.results]]
value = "{ id: pin:3, v: 5 }"

[[test.results]]
value = "{ id: pin:4, note: 'no_v' }"

[[test.results]]
value = "{ id: pin:5, v: 1 }"

[[test.results]]
value = "{ id: pin:6, o: { x: 1, y: 2 } }"

[[test.results]]
value = "[{ id: pin:1, s: 'hello', v: 10 }, { id: pin:2, v: 20 }]"

[[test.results]]
value = "[{ id: pin:3, v: 5 }, { id: pin:4, note: 'no_v' }, { id: pin:5, v: 1 }, { id: pin:6, o: { x: 1, y: 2 } }]"

[[test.results]]
value = "[{ id: pin:5, v: 1 }]"

[[test.results]]
value = "[{ id: pin:5, v: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pin:1, s: 'hello', v: 10 }, { id: pin:2, v: 20 }, { id: pin:3, v: 5 }, { id: pin:4, note: 'no_v' }, { id: pin:5, v: 1 }, { id: pin:6, o: { x: 1, y: 2 } }]"

[[test.results]]
value = "[{ id: pin:3, v: 5 }, { id: pin:5, v: 1 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: pin:1, s: 'hello', v: 10 }, { id: pin:2, v: 20 }, { id: pin:3, v: 5 }, { id: pin:5, v: 1 }]"

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    TableScan [ctx: Db] [table: pin, direction: Forward, predicate: v INSIDE [10, 20], pre_decode_filter: yes]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    TableScan [ctx: Db] [table: pin, direction: Forward, predicate: v NOTINSIDE [99], pre_decode_filter: yes]\n''''

[[test.results]]
value = ''''SelectProject [ctx: Db] [projections: *]\n    TableScan [ctx: Db] [table: pin, direction: Forward, predicate: v INSIDE 3 .. 7, pre_decode_filter: no (unsupported predicate)]\n''''
*/

CREATE ONLY pin:1 SET v = 10, s = 'hello';
CREATE ONLY pin:2 SET v = 20;
CREATE ONLY pin:3 SET v = 5;
CREATE ONLY pin:4 SET note = 'no_v';
CREATE ONLY pin:5 SET v = 1;
CREATE ONLY pin:6 SET o = { x: 1, y: 2 };

SELECT * FROM pin WHERE v INSIDE [10, 20, 30];
SELECT * FROM pin WHERE v NOTINSIDE [10, 20, 30];
SELECT * FROM pin WHERE v INSIDE [1, 2f, 3dec];
SELECT * FROM pin WHERE v INSIDE [1, 1f, 1dec];
SELECT * FROM pin WHERE v INSIDE [];
SELECT * FROM pin WHERE v NOTINSIDE [];
SELECT * FROM pin WHERE v INSIDE 1..10;
SELECT * FROM pin WHERE o INSIDE [{ x: 1 }];

SELECT * FROM pin WHERE v INSIDE [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 27, 28, 29, 30, 31, 32, 33];

EXPLAIN SELECT * FROM pin WITH NOINDEX WHERE v INSIDE [10, 20];
EXPLAIN SELECT * FROM pin WITH NOINDEX WHERE v NOTINSIDE [99];
EXPLAIN SELECT * FROM pin WITH NOINDEX WHERE v INSIDE 3..7;
