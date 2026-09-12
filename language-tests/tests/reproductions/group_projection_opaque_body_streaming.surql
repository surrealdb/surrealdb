/**
[env]
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = """
Half of a pair. Same queries as
`group_projection_opaque_body_legacy.surql`, pinned to the streaming
planner; the two files' expected results are the divergence.

A grouped projection reduces an expression the row cannot influence to a
single value — see `group_projection_reduces_per_group.surql`, where every
strategy agrees, including for builtin calls such as `math::abs(-7)`.
Deciding that requires reading the expression to its leaves, and two of
these three have a leaf plan time cannot read: a subquery and a closure
call each run a body that may reach the row through `$parent`. Not being
able to prove they don't, the planner treats them as reading the row, so
they collect one element per row here where the legacy path reports the
single value.

A user-defined function is the third, and it is no longer one of them: its
body is inlined into the plan before the projection is read, so what
remains is an ordinary expression judged by its own leaves. A body that
does reach the row inlines into one that reads it, and collects.

That direction is the safe one. Treating a row-independent expression as
row-reading costs an array where one value would have done, which is this
divergence. Treating a row-reading expression as row-independent would
report one row's value as the whole group's and drop the rest.

Widening this to the remaining two needs the body's row access decided,
not assumed — for a subquery by establishing it is uncorrelated. Until
then the legacy expectations are the intended contract.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ g: 1, id: t:1 }]"

[[test.results]]
value = "[{ g: 1, id: t:2 }]"

[[test.results]]
value = "[{ g: 1, k: [5, 5] }]"

[[test.results]]
value = "[{ g: 1, k: 5 }]"

[[test.results]]
value = "[{ g: 1, k: [5, 5] }]"
*/

DEFINE FUNCTION fn::five() { RETURN 5; };

CREATE t:1 SET g = 1;
CREATE t:2 SET g = 1;

-- An uncorrelated subquery: it reads no outer row, but establishing that
-- is not something the shape alone settles.
SELECT (SELECT VALUE 5 FROM ONLY t:1) AS k, g FROM t GROUP BY g;

-- A user-defined function, whose body is inlined before the
-- projection is read.
SELECT fn::five() AS k, g FROM t GROUP BY g;

-- A closure, likewise a body rather than a builtin.
SELECT (|$x: int| { RETURN $x; })(5) AS k, g FROM t GROUP BY g;
