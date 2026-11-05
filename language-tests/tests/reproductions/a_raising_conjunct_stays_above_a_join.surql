/**
[env]
namespace = true
database = true

[test]
reason = """
A conjunct judged above a join is judged for the rows the join kept; moved
onto an input, it is judged for every row that input yields, the ones the join
drops included. A term that only reads answers the same either way. One that
can raise does not: `l:2` has no match, so the semi join drops it and the
`THROW` is never reached — until the term is moved beneath the join, where it
raises on a row the statement never returns.

A term that can raise is never moved beneath a join, whether the planner splits
a filter around the join, splits the join's residual, or leaves behind the
conjuncts around a membership it turns into a join. It stays where the join has
already filtered, and a term that only reads moves onto an input as an ordinary
field comparison does.

`AND` short-circuits left to right, so which side of the join a term belongs
on is a question of where it was written. A conjunct written before a lifted
membership is already judged for every row that reaches it, so it moves beneath
the join and raises exactly where the per-row form does. Only a conjunct written
after the lifted one waits above.

Two lifted conjuncts make it a place rather than a side: a term written
between them is judged for the rows the first membership kept and before the
second is asked, so it stands between the two joins. The same three conjuncts
in two orders answer differently.

A term that cannot raise sinks below the joins — but only past joins that
raise nothing themselves. A join builds its right side only once its probe
yields a row, so a term that refuses every probe row takes the build with it,
and with a lift whose own pipeline can raise that is an error suppressed.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[l:1]"

[[test.results]]
value = "[l:1]"

[[test.results]]
error = "An error occurred: bad"

[[test.results]]
error = "An error occurred: one"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
error = "An error occurred: between"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"

[[test.results]]
error = "Could not cast into `int` using input `'x'`"
*/

DEFINE TABLE l SCHEMALESS;
DEFINE TABLE rr SCHEMALESS;
CREATE l:1 SET k = 1, ok = true  RETURN NONE;
CREATE l:2 SET k = 2, ok = false RETURN NONE;
CREATE rr:1 SET k = 1 RETURN NONE;

-- Decorrelation lifts the `IN` and leaves the `THROW` behind: it must not
-- leave it beneath the join it just built.
SELECT VALUE id FROM l WHERE k IN (SELECT VALUE k FROM rr) AND (IF ok THEN true ELSE THROW 'bad' END);

-- The same term arriving from outside, pushed into an identity subquery that
-- already holds the join.
SELECT VALUE id FROM (SELECT * FROM l WHERE k IN (SELECT VALUE k FROM rr)) WHERE (IF ok THEN true ELSE THROW 'bad' END);

-- Written before the `IN`, the same term is reached for every row: the
-- per-row form raises on `l:2` too, so keeping it above the join would be the
-- divergence.
SELECT VALUE id FROM l WHERE (IF ok THEN true ELSE THROW 'bad' END) AND k IN (SELECT VALUE k FROM rr);

-- One conjunct on each side of the lifted one. The first is reached for every
-- row and raises; were the second judged first, the error would name `two`.
SELECT VALUE id FROM l
	WHERE (IF ok THEN true ELSE THROW 'one' END)
	AND k IN (SELECT VALUE k FROM rr)
	AND (IF ok THEN true ELSE THROW 'two' END);

-- Between two lifted conjuncts. `l:2` matches `m1` and not `m2`, so the
-- per-row form reaches the `THROW` before the second membership is asked.
DEFINE TABLE m1 SCHEMALESS;
DEFINE TABLE m2 SCHEMALESS;
CREATE m1:1 SET k = 2 RETURN NONE;
CREATE m2:1 SET k = 1 RETURN NONE;
SELECT VALUE id FROM l
	WHERE k IN (SELECT VALUE k FROM m1)
	AND (IF ok THEN true ELSE THROW 'between' END)
	AND k IN (SELECT VALUE k FROM m2);

-- The same three conjuncts with the term written last: `m2` refuses `l:2`
-- first, so nothing raises.
SELECT VALUE id FROM l
	WHERE k IN (SELECT VALUE k FROM m1)
	AND k IN (SELECT VALUE k FROM m2)
	AND (IF ok THEN true ELSE THROW 'between' END);

-- The lifted subquery itself can raise, so `false` may not sink onto the
-- probe: an empty probe skips the join's build and the error with it.
DEFINE TABLE raiser SCHEMALESS;
CREATE raiser:1 SET k = 1, bad = 'x' RETURN NONE;
SELECT VALUE id FROM l WHERE k IN (SELECT VALUE k FROM raiser WHERE <int> bad > 0) AND false;
SELECT VALUE id FROM l WHERE k IN (SELECT VALUE k FROM raiser WHERE <int> bad > 0);
