/**
[env]
namespace = true
database = true

[test]
reason = """
A record created by a statement is not visited by that same statement's scan, in
either key direction. The streaming write path relies on this to interleave reads
and writes batch-at-a-time instead of draining its input, so the property is
pinned rather than assumed. The two cases use separate tables so neither
statement can observe the other's records.
"""

[[test.results]]
value = "[{ id: ahead:1 }]"

[[test.results]]
value = "[{ id: ahead:2 }]"

[[test.results]]
value = "[{ id: ahead:1, spawn: [ahead:9], touched: true }, { id: ahead:2, touched: true }]"

[[test.results]]
value = "[{ id: ahead:1, touched: true }, { id: ahead:2, touched: true }, { id: ahead:9, touched: NONE }]"

[[test.results]]
value = "[{ id: behind:5 }]"

[[test.results]]
value = "[{ id: behind:6 }]"

[[test.results]]
value = "[{ id: behind:5, spawn: [behind:4], touched: true }, { id: behind:6, touched: true }]"

[[test.results]]
value = "[{ id: behind:4, touched: NONE }, { id: behind:5, touched: true }, { id: behind:6, touched: true }]"
*/

-- Ahead of the cursor: ahead:9 sorts after both scanned records.
CREATE ahead:1;
CREATE ahead:2;
UPDATE ahead SET touched = true, spawn = (IF id = ahead:1 { (CREATE ahead:9).id } ELSE { NONE });
SELECT id, touched FROM ahead ORDER BY id;

-- Behind the cursor: behind:4 sorts before both scanned records.
CREATE behind:5;
CREATE behind:6;
UPDATE behind SET touched = true, spawn = (IF id = behind:5 { (CREATE behind:4).id } ELSE { NONE });
SELECT id, touched FROM behind ORDER BY id;
