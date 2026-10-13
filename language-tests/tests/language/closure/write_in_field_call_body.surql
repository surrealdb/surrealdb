/**
[env]
namespace = true
database = true
planner-strategy = ["compute-only", "all-ro", "best-effort-ro"]

[test]
reason = """
A closure stored in an object field hides its body from the planner, so a
statement invoking it through an unregistered method name ($o.w()) must be
classified read-write: the body may write even though every plan-visible
expression is read-only. The write must land on every planner strategy.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t2:1 }]"

[[test.results]]
value = "[{ id: t2:1 }]"
*/
DEFINE TABLE OVERWRITE t2;

LET $o = { w: || (CREATE t2:1) };
RETURN $o.w();

SELECT * FROM t2;
