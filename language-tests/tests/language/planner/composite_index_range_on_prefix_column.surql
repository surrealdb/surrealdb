/**
[env]
planner-strategy = ["all-ro"]

[test]
reason = "Range bounds on the leading column of a compound index must cover entries that encode the remaining columns: `>` must exclude the bound value entirely and `<=` must not drop it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: 1, id: rec:r1 }]"

[[test.results]]
value = "[{ a: 1, b: 2, id: rec:r2 }]"

[[test.results]]
value = "[{ a: 2, b: 1, id: rec:r3 }]"

[[test.results]]
value = "[{ a: 2, b: 2, id: rec:r4 }]"

[[test.results]]
value = "[{ a: 3, b: 1, id: rec:r5 }]"

[[test.results]]
value = "[{ a: 3, b: 2, id: rec:r6 }]"

[[test.results]]
value = "[{ a: 1, b: 1, id: urec:r1 }]"

[[test.results]]
value = "[{ a: 1, b: 2, id: urec:r2 }]"

[[test.results]]
value = "[{ a: 2, b: 1, id: urec:r3 }]"

[[test.results]]
value = "[{ a: 2, b: 2, id: urec:r4 }]"

[[test.results]]
value = "[{ a: 3, b: 1, id: urec:r5 }]"

[[test.results]]
value = "[{ a: 3, b: 2, id: urec:r6 }]"

[[test.results]]
value = "[2, 2, 3, 3]"

[[test.results]]
value = "[2, 2, 3, 3]"

[[test.results]]
value = "[1, 1, 2, 2]"

[[test.results]]
value = "[1, 1, 2, 2]"

[[test.results]]
value = "[2, 2]"

[[test.results]]
value = "[2, 2, 1, 1]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[2, 2, 3, 3]"

[[test.results]]
value = "[1, 1, 2, 2]"

[[test.results]]
value = "[2, 2]"

[[test.results]]
value = "[2, 2, 1, 1]"
*/

DEFINE INDEX idx_a_b ON rec FIELDS a, b;
DEFINE INDEX uniq_a_b ON urec FIELDS a, b UNIQUE;

CREATE rec:r1 SET a = 1, b = 1;
CREATE rec:r2 SET a = 1, b = 2;
CREATE rec:r3 SET a = 2, b = 1;
CREATE rec:r4 SET a = 2, b = 2;
CREATE rec:r5 SET a = 3, b = 1;
CREATE rec:r6 SET a = 3, b = 2;

CREATE urec:r1 SET a = 1, b = 1;
CREATE urec:r2 SET a = 1, b = 2;
CREATE urec:r3 SET a = 2, b = 1;
CREATE urec:r4 SET a = 2, b = 2;
CREATE urec:r5 SET a = 3, b = 1;
CREATE urec:r6 SET a = 3, b = 2;

-- `>` on the leading column must exclude every entry whose leading value equals the bound
SELECT VALUE a FROM rec WHERE a > 1 ORDER BY a;
SELECT VALUE a FROM rec WHERE a >= 2 ORDER BY a;

-- `<=` on the leading column must keep every entry whose leading value equals the bound
SELECT VALUE a FROM rec WHERE a <= 2 ORDER BY a;
SELECT VALUE a FROM rec WHERE a < 3 ORDER BY a;

-- both bounds at once, and in reverse order
SELECT VALUE a FROM rec WHERE a > 1 AND a <= 2 ORDER BY a;
SELECT VALUE a FROM rec WHERE a <= 2 ORDER BY a DESC;

-- ranges on the trailing column behind an equality prefix stay correct
SELECT VALUE b FROM rec WHERE a = 2 AND b > 1 ORDER BY b;
SELECT VALUE b FROM rec WHERE a = 2 AND b <= 1 ORDER BY b;

-- the same bounds over a compound UNIQUE index
SELECT VALUE a FROM urec WHERE a > 1 ORDER BY a;
SELECT VALUE a FROM urec WHERE a <= 2 ORDER BY a;
SELECT VALUE a FROM urec WHERE a > 1 AND a <= 2 ORDER BY a;
SELECT VALUE a FROM urec WHERE a <= 2 ORDER BY a DESC;
