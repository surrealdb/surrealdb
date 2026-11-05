/**
[env]
namespace = true
database = true

[test]
reason = """
A BREAK or CONTINUE raised while computing a LET's value belongs to the
enclosing loop, so it travels through the binding to reach it — the same answer
whether the value plan raises the signal as it starts or partway through
producing rows. With no loop to reach, the statement boundary rejects it.
"""

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[foo:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[bar:1, bar:3]"

*/

-- No loop to reach, so the statement boundary rejects it.
LET $a = BREAK;

-- Inside a loop the signal reaches the loop: only $i = 1 completes the body.
FOR $i IN [1, 2, 3] {
	LET $b = IF $i == 2 { BREAK } ELSE { $i };
	CREATE type::record('foo', $i);
};
SELECT VALUE id FROM foo;

-- CONTINUE likewise reaches the loop, skipping the rest of that iteration.
FOR $i IN [1, 2, 3] {
	LET $c = IF $i == 2 { CONTINUE } ELSE { $i };
	CREATE type::record('bar', $i);
};
SELECT VALUE id FROM bar;
