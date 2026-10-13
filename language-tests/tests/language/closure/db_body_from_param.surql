/**
[env]
namespace = true
database = true
planner-strategy = ["compute-only", "all-ro", "best-effort-ro"]

[test]
reason = """
A closure that arrives as a runtime value (bound to a param, stored in an
object field, or created inline) hides its body from the planner, so the
executor must build the execution context from the session's ns/db selection
rather than from the plan's declared context level. A database-touching body
must work when the closure is invoked directly ($f()), through an object
field ($o.foo()), and inline ((|| ...)()), on every planner strategy.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, name: 'a' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['a']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['a']"

[[test.results]]
value = "['a']"
*/
DEFINE TABLE OVERWRITE t;
CREATE t:1 SET name = 'a';

LET $f = || (SELECT VALUE name FROM t:1);
RETURN $f();

LET $o = { foo: || (SELECT VALUE name FROM t:1) };
RETURN $o.foo();

RETURN (|| (SELECT VALUE name FROM t:1))();
