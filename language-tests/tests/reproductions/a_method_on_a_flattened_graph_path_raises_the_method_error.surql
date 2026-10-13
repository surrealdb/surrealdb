/**
[env]
namespace = true
database = true

[test]
reason = "A closure-taking method called on a flattened graph path raises the method error for the value it is called on, whether or not the closure captures a name; it is not declined by the engine."

[[test.results]]
value = "[{ id: a:1 }]"

[[test.results]]
value = "[{ id: b:1 }]"

[[test.results]]
value = "[{ id: b:2 }]"

[[test.results]]
value = "[{ id: rel:1, in: a:1, out: b:1, w: 1 }]"

[[test.results]]
value = "[{ id: rel:2, in: a:1, out: b:2, w: 10 }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "There was a problem running the filter() function. no such method found for the int type"

[[test.results]]
error = "There was a problem running the map() function. no such method found for the int type"

[[test.results]]
error = "There was a problem running the filter() function. no such method found for the int type"
*/

CREATE a:1;
CREATE b:1;
CREATE b:2;
RELATE a:1->rel:1->b:1 SET w = 1;
RELATE a:1->rel:2->b:2 SET w = 10;
LET $m = 8;
SELECT ->rel.w.filter(|$e| $e >= $m) AS ws FROM a:1;
SELECT ->rel.w.map(|$e| $e + $m) AS ws FROM a:1;
SELECT ->rel.w.filter(|$e| $e >= 8) AS ws FROM a:1;
