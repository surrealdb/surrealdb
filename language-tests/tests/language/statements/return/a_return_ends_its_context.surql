/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A RETURN sets the value of the context it stands in and ends it. The contexts are a statement of
the request (or of a transaction), a function body, a closure body, and each evaluation a statement
makes over one row or one value: a WHERE per row, a projection per row, a SET, CONTENT or MERGE
value per record, a write's RETURN clause, a field's VALUE, DEFAULT and ASSERT, a permission and an
event. Everything else is no context, and a RETURN there ends the nearest context around it with
its value: an operand, an array or object element, an IF condition or arm, a block, a FOR body or
iterable, a parenthesised statement, a LET's value, a FROM source, LIMIT, START and TIMEOUT, the
target of a write and the rows an INSERT writes. A RETURN in a per-row context never ends the
statement around it.
"""

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
value = "2"

[[test.results]]
value = "true"

[[test.results]]
value = "'c'"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "[1, 5]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[1, 5]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[1, 5]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ id: person:1 }, { id: person:5 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[true]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[1, 9]"

[[test.results]]
value = "['small', 'big']"

[[test.results]]
value = "[2, 2]"

[[test.results]]
value = "[0, 3]"

[[test.results]]
value = "[{ a: 1, b: 10 }, { a: 2, b: 20 }]"

[[test.results]]
value = "1"

[[test.results]]
value = "1"

[[test.results]]
value = "1s"

[[test.results]]
value = "7"

[[test.results]]
value = "[1, 2]"

[[test.results]]
error = "The table 'for_src' does not exist"

[[test.results]]
value = "person:5"

[[test.results]]
value = "person:1"

[[test.results]]
value = "up:1"

[[test.results]]
value = "t:9"

[[test.results]]
value = "person:5"

[[test.results]]
value = "2"

[[test.results]]
value = "[person:1, person:5]"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "['knows', 'person']"

[[test.results]]
value = "[{ id: ts:1, x: 1, y: 5 }]"

[[test.results]]
error = "Cannot use 7 in a CONTENT clause"

[[test.results]]
error = "Cannot use 3 in a MERGE clause"

[[test.results]]
value = "[ts:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "'stop'"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "105"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['after small', 'big']"

[[test.results]]
value = "105"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "['small', 'big']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ fa: 1, fd: 6, fv: 5, id: fvt:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: evt:1 }]"

[[test.results]]
value = "[3]"
*/

-- Operands, elements, blocks, IF and FOR in value position: the statement ends.
RETURN 1 + (RETURN 2);
RETURN 1 + { RETURN 2 };
RETURN [1, (RETURN 2), 3];
RETURN { a: (RETURN 2) };
IF RETURN true THEN 'yes' ELSE 'no' END;
IF true { IF (RETURN 'c') { 'a' } ELSE { 'b' } } ELSE { 'z' };
RETURN (IF true { RETURN 1 } ELSE { 2 }) + 10;
RETURN { IF true { RETURN 1 }; 2 } + 10;
RETURN 10 + (FOR $i IN [1, 2] { RETURN $i });
FOR $i IN [1, 2] { RETURN $i };

-- A LET's value: the statement ends and nothing is bound.
LET $x = 1 + (RETURN 2);
RETURN $x;
RETURN { LET $q = 1 + (RETURN 4); $q + 100 };

-- A WHERE is a context per row, and so is the WHERE of a graph lookup. An element filter's
-- condition (`[WHERE ...]`) is an expression, no statement's evaluation, so it is no context.
SELECT VALUE id FROM [{ id: 1, a: 0 }, { id: 2, a: 1 }, { id: 3, a: 1.5 }, { id: 4, a: 2 }, { id: 5, a: 3 }] WHERE { IF a > 2 { RETURN true }; IF a < 1 { RETURN true }; false };
SELECT VALUE id FROM [{ id: 1, a: 0 }, { id: 5, a: 3 }] WHERE (IF a > 2 { RETURN true } ELSE { false });
SELECT VALUE id FROM [{ id: 1, a: 0 }, { id: 5, a: 3 }] WHERE (RETURN true);
SELECT VALUE id FROM [{ id: 1, a: 0 }, { id: 5, a: 3 }] WHERE a > 2 AND (RETURN true);
SELECT VALUE id FROM [{ id: 1, a: 0 }, { id: 5, a: 3 }] WHERE (RETURN false) OR true;
RETURN [1, 2, 3][WHERE $this > 1 AND (RETURN true)];
CREATE person:1, person:5;
RELATE person:1->knows->person:5 RETURN NONE;
SELECT VALUE ->knows[WHERE (RETURN true)]->? FROM person:1;
SELECT VALUE ->(knows WHERE (RETURN false) OR true)->? FROM person:1;
RETURN (SELECT VALUE x FROM [{ x: 1 }] WHERE (RETURN true)) + [9];

-- A projection is a context per row.
SELECT VALUE { IF a > 2 { RETURN 'big' }; 'small' } FROM [{ a: 0 }, { a: 3 }];
SELECT VALUE 1 + (RETURN 2) FROM [{ a: 0 }, { a: 3 }];
SELECT VALUE { RETURN a } + 1 FROM [{ a: 0 }, { a: 3 }];
SELECT a, 1 + (RETURN a * 10) AS b FROM [{ a: 1 }, { a: 2 }];

-- Clauses a statement evaluates once are no context: the statement ends.
SELECT VALUE a FROM [{ a: 0 }, { a: 3 }] LIMIT (RETURN 1);
SELECT VALUE a FROM [{ a: 0 }, { a: 3 }] START (RETURN 1);
SELECT VALUE a FROM [{ a: 0 }, { a: 3 }] TIMEOUT (RETURN 1s);
SELECT * FROM (RETURN 7);
FOR $i IN (RETURN [1, 2]) { CREATE for_src SET i = $i };
SELECT VALUE i FROM for_src;

-- The target of a write is no context: the statement ends before it writes.
DELETE (RETURN person:5);
UPDATE (RETURN person:1) SET z = 1;
UPSERT (RETURN up:1) SET z = 1;
CREATE (RETURN t:9);
RELATE person:1->likes->(RETURN person:5);
INSERT INTO ins [{ a: 1 }, { a: (RETURN 2) }];
SELECT VALUE id FROM person ORDER BY id;
SELECT VALUE z FROM person ORDER BY id;
RETURN (INFO FOR DB).tables.keys();

-- A value a write stores, and its RETURN clause, are contexts per record.
CREATE ts:1 SET x = (RETURN 1) + 1, y = 5;
CREATE ts:2 CONTENT { x: (RETURN 7) };
UPDATE ts:1 MERGE { m: 1 + (RETURN 3) };
UPDATE ts SET z = 1 WHERE (RETURN true) RETURN VALUE 1 + (RETURN id);
FOR $i IN [1, 2] { CREATE fl SET i = 1 + (RETURN $i) };
SELECT VALUE i FROM fl ORDER BY i;
FOR $i IN [1, 2, 3] { CREATE stop SET i = $i; IF $i = 2 { RETURN 'stop' } };
SELECT VALUE i FROM stop ORDER BY i;

-- Function and closure bodies are contexts.
DEFINE FUNCTION fn::f($x: int) { RETURN 1 + (RETURN $x) };
RETURN fn::f(5) + 100;
DEFINE FUNCTION fn::g($x: int) { LET $y = { IF $x > 2 { RETURN 'big' }; 'small' }; RETURN 'after ' + $y };
RETURN [fn::g(1), fn::g(3)];
RETURN (|$x| 1 + (RETURN $x))(5) + 100;
RETURN [1, 2, 3].filter(|$v| $v > (RETURN 1));
RETURN [1, 2].map(|$x| { IF $x > 1 { RETURN 'big' }; 'small' });

-- Stored clauses are contexts per record.
DEFINE FIELD fv ON fvt VALUE 1 + (RETURN 5);
DEFINE FIELD fd ON fvt DEFAULT 1 + (RETURN 6);
DEFINE FIELD fa ON fvt ASSERT (RETURN true) AND false;
CREATE fvt:1 SET fa = 1;
DEFINE EVENT ev ON evt WHEN (RETURN true) AND false THEN { CREATE evlog SET v = 1 + (RETURN 3) };
CREATE evt:1;
SELECT VALUE v FROM evlog;
