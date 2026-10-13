/**
[test]
reason = "Plan-time parameter substitution must not inline values whose literal form evaluates differently: a set becomes an array literal (sets and arrays compare unequal), and a closure literal drops its captures. No planner-strategy, so all engines must agree"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: doc:a, tags: {'x', 'y'} }]"

[[test.results]]
value = "[{ id: doc:b, tags: {'x',} }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{'x', 'y'}"

[[test.results]]
value = "[{ id: doc:a }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: num:one, vals: [1, 2, 3] }]"

[[test.results]]
value = "[{ id: num:two, vals: [1, 2] }]"

[[test.results]]
value = "[{ id: num:one }]"
*/

DEFINE FIELD tags ON TABLE doc TYPE set<string>;
CREATE doc:a SET tags = {'x', 'y'};
CREATE doc:b SET tags = {'x',};

-- Set-valued param: `tags = $s` must match doc:a (a lossy substitution
-- inlines an array literal, which never equals a set, returning [])
LET $s = doc:a.tags;
RETURN $s;
SELECT id FROM doc WHERE tags = $s;

-- Closure-valued param: the captured $y must survive
LET $y = 2;
LET $f = |$x: number| $x > $y;
CREATE num:one SET vals = [1, 2, 3];
CREATE num:two SET vals = [1, 2];
SELECT id FROM num WHERE vals.any($f) ORDER BY id;
