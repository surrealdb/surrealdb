/**
[env]
namespace = true
database = true

[test]
reason = """
A FROM source is evaluated once for the statement and is no context: a RETURN raised while
evaluating it ends the statement with the RETURN's value, and nothing of the statement's pipeline
runs. A record id's key element is no context either. A WHERE is a context, so a RETURN in a
record id in a WHERE decides the row. BREAK and CONTINUE with no enclosing FOR have no loop to act
on there and are invalid control flow. Written inside a FOR's body, a FROM source is part of the
loop: the signal reaches the FOR, which ends or continues as the signal says, and the writes the
loop had made stay. A BREAK in a key written inside a FOR's body reaches that FOR as well.
"""

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[{ a: 9, id: t:[1] }]"

[[test.results]]
value = "t:1"

[[test.results]]
value = "t:1"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "t:1"

[[test.results]]
value = "2"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "[t:1, t:2]"

[[test.results]]
value = "t:1"

[[test.results]]
value = "t:1"

[[test.results]]
value = "'t'"

[[test.results]]
value = "[t:1]"

[[test.results]]
value = "1"

[[test.results]]
value = "[{ a: 1, id: t:1 }, { a: 2, id: t:2 }, { a: 9, id: t:[1] }]"

[[test.results]]
value = "t:1"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 7, id: t:{ k: 1 } }]"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1 }]"

*/

CREATE t:1 SET a = 1;
CREATE t:2 SET a = 2;
CREATE t:[1] SET a = 9;

-- The source expression itself signals RETURN, which ends the statement.
SELECT * FROM (RETURN t:1);
SELECT VALUE a FROM (RETURN t:1);
SELECT * FROM (RETURN [t:1, t:2]);
SELECT * FROM ONLY (RETURN t:1);

-- No record id involved: the statement answers whatever RETURN named.
SELECT * FROM (RETURN 1 + 1);

-- The clauses above and below the source do not run.
SELECT * FROM (RETURN [t:1, t:2]) WHERE a > 1;
SELECT * FROM (RETURN [t:1, t:2]) ORDER BY a DESC LIMIT 1;

-- A block body, and a block whose last statement binds through a LET.
SELECT * FROM { RETURN t:1 };
SELECT * FROM { LET $x = t:1; RETURN $x };

-- Raised from inside a function's argument, which resolves its source at run
-- time. The signal leaves the call before the function runs, and ends the
-- statement.
SELECT * FROM type::table((RETURN "t"));
SELECT * FROM array::flatten([(RETURN [t:1])]);

-- Raised from inside a record id key, which is no context; in a WHERE it decides the row.
SELECT * FROM t:[(RETURN 1)];
SELECT * FROM t WHERE id = t:[(RETURN 1)];

-- The signal ends the LET's statement, and the LET binds nothing.
LET $r = (SELECT VALUE a FROM (RETURN t:1));
RETURN $r;

-- With no FOR around it, BREAK/CONTINUE in a source position is invalid control flow.
-- Inside a FOR, a source is part of the loop body: the FOR ends.
SELECT * FROM (BREAK);
SELECT * FROM (CONTINUE);
FOR $i IN [1] { SELECT * FROM (BREAK); };

-- An object-keyed record id key is no context either.
CREATE t:{ k: 1 } SET a = 7;
RETURN t:{ a: (RETURN 1) };
SELECT * FROM t:{ k: (RETURN 1) };

-- A BREAK inside a record id key written in a FOR's body ends that FOR, and the
-- write the iteration had made before it stays.
FOR $i IN [1, 2] { CREATE u:[$i] SET a = $i; SELECT * FROM t:[(BREAK)]; };
SELECT count() FROM u GROUP ALL;
