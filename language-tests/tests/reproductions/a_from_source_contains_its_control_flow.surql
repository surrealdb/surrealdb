/**
[env]
namespace = true
database = true

[test]
reason = """
A FROM source is a value position: a RETURN raised while evaluating it names
the value to iterate, so the statement still answers with rows and the rest of
the pipeline still runs. The signal must not escape and become the enclosing
statement's own result. BREAK and CONTINUE have no loop to act on there and
stay invalid control flow, including inside an enclosing FOR: the loop raises
rather than taking the signal as its own, and the writes it had made are rolled
back with the statement.

A record id's key is the one part of it that is an expression, so it draws the
same boundary per key, for an array key and an object key alike.
"""

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[{ a: 9, id: t:[1] }]"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[{ a: 1, id: t:1 }, { a: 2, id: t:2 }]"

[[test.results]]
value = "{ a: 1, id: t:1 }"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[{ a: 2, id: t:2 }]"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "['t']"

[[test.results]]
value = "[{ a: 1, id: t:1 }]"

[[test.results]]
value = "[{ a: 9, id: t:[1] }]"

[[test.results]]
value = "[{ a: 9, id: t:[1] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1]"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[{ a: 7, id: t:{ k: 1 } }]"

[[test.results]]
value = "t:{ a: 1 }"

[[test.results]]
value = "[{ a: 7, id: t:{ k: 1 } }]"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "The table 'u' does not exist"
*/

CREATE t:1 SET a = 1;
CREATE t:2 SET a = 2;
CREATE t:[1] SET a = 9;

-- The source expression itself signals RETURN.
SELECT * FROM (RETURN t:1);
SELECT VALUE a FROM (RETURN t:1);
SELECT * FROM (RETURN [t:1, t:2]);
SELECT * FROM ONLY (RETURN t:1);

-- No record id involved: the value is whatever RETURN named.
SELECT * FROM (RETURN 1 + 1);

-- The clauses above and below the source still run.
SELECT * FROM (RETURN [t:1, t:2]) WHERE a > 1;
SELECT * FROM (RETURN [t:1, t:2]) ORDER BY a DESC LIMIT 1;

-- A block body, and a block whose last statement binds through a LET.
SELECT * FROM { RETURN t:1 };
SELECT * FROM { LET $x = t:1; RETURN $x };

-- Raised from inside a function's argument, which resolves its source at run
-- time. The signal leaves the call before the function runs, so the source is
-- the value RETURN named rather than that function's result: the table name is
-- iterated as the plain string it is.
SELECT * FROM type::table((RETURN "t"));
SELECT * FROM array::flatten([(RETURN [t:1])]);

-- Raised from inside a record id key, which names that key element.
SELECT * FROM t:[(RETURN 1)];
SELECT * FROM t WHERE id = t:[(RETURN 1)];

-- The signal does not escape the statement the SELECT is written in.
LET $r = (SELECT VALUE a FROM (RETURN t:1));
RETURN $r;

-- BREAK/CONTINUE have no loop to act on in a source position.
SELECT * FROM (BREAK);
SELECT * FROM (CONTINUE);
FOR $i IN [1] { SELECT * FROM (BREAK); };

-- An object-keyed record id draws the boundary per key element too.
CREATE t:{ k: 1 } SET a = 7;
RETURN t:{ a: (RETURN 1) };
SELECT * FROM t:{ k: (RETURN 1) };

-- A BREAK inside a record id key raises rather than breaking the enclosing
-- loop: the loop's own write is rolled back with the statement, so the table
-- it would have created is absent.
FOR $i IN [1, 2] { CREATE u:[$i] SET a = $i; SELECT * FROM t:[(BREAK)]; };
SELECT count() FROM u GROUP ALL;
