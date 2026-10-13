/**
[env]
namespace = false
database = false
planner-strategy = ["compute-only", "all-ro", "best-effort-ro"]

[test]
reason = """
A pure closure invoked in a session with no namespace or database selected
must evaluate at root level, both directly ($f()) and through an object field
($o.inc()), on every planner strategy. The executor upgrades the context from
the session's ns/db selection opportunistically, so a session without one must
not start failing.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"
*/
LET $f = |$x: number| $x + 1;
RETURN $f(1);

LET $o = { inc: |$x: number| $x + 1 };
RETURN $o.inc(1);
