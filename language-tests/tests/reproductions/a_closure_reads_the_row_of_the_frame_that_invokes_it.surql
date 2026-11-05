/**
[env]
namespace = true
database = true

[test]
reason = "A closure captures the LET and function parameters it reads where the literal is written, and nothing of the row there: its body reads the row of the frame that invokes it, its document root or, where it has none, its current value, as `$this`, `@` and its fields. A closure created in one frame and invoked in another reads the row of the second, and one invoked by a method in a frame with no row reads none. A closure-free method argument in a frame with no row reads the receiver as its current value and `$this`."

[[test.results]]
value = "[{ id: t:1, n: 10 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[11]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[11]]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot perform addition with 'int' and 'none'"

[[test.results]]
value = "[[11]]"

[[test.results]]
value = "[1, 10]"

[[test.results]]
value = "[1, 5, 10, 15, 20]"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "[[NONE], [NONE]]"

[[test.results]]
value = "[{ k: 3 }, { k: 4 }, [3, 4]]"

[[test.results]]
value = "[1, 5, [1, 5]]"
*/

CREATE t:1 SET n = 10;
LET $m = 1;
LET $h1 = |$a| $a + $m;
LET $m = 5;
RETURN $h1(1);
LET $h2 = |$a| $a + n;
SELECT VALUE [1].map($h2) FROM t;
LET $h3 = |$a| $a + $this.n;
SELECT VALUE [1].map($h3) FROM t;
LET $f = (SELECT VALUE |$a| $a + n FROM t:1)[0];
RETURN $f(1);
SELECT VALUE [1].map(|$a| $a + n) FROM t;
RETURN [1, 10].filter(|$e| $e >= lim);
RETURN [1, 5, 10, 15, 20].filter(|$e| $this = NONE);
RETURN [{ k: 3 }, { k: 4 }].map(|$e| k);
RETURN [{ k: 3 }, { k: 4 }].map(|$e| [$e].map(|$f| k));
RETURN [{ k: 3 }, { k: 4 }].concat([k]);
RETURN [1, 5].concat([$this]);
