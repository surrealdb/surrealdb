/**
[test]
reason = "The body is written in `SELECT VALUE id FROM x WHERE v[WHERE $this >= $m] != [] ORDER BY id`. Each name and row it reads takes the value of the frame where the body is written."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: x:1, v: [10, 3] }]"

[[test.results]]
value = "[{ id: x:2, v: [1] }]"

[[test.results]]
value = "[x:1]"
*/
LET $m = 8;
DEFINE INDEX ix ON x FIELDS v;
CREATE x:1 SET v = [10, 3];
CREATE x:2 SET v = [1];
SELECT VALUE id FROM x WHERE v[WHERE $this >= $m] != [] ORDER BY id;
