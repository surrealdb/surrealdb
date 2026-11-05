/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A BREAK or CONTINUE targets the nearest FOR it is written in. Every clause
written in the loop body passes it to that loop: a SELECT's WHERE, projection,
LIMIT and START, an element filter, a graph-step filter, the RETURN clause of a
write, the values of an INSERT, a SET or CONTENT value, a FROM source, the WHERE
of an UPDATE or DELETE, a value block, a parenthesised expression, a value IF
and a record id's key element. A write the exit leaves keeps the records it had
written: the current record is written when the exit comes after its write (a
RETURN clause) and not when it comes before (a SET or CONTENT value, a WHERE),
and over several records the ones before the exit stay written. Later records
are not visited, and CONTINUE differs from BREAK only in that the loop goes on.
A frame the loop is not written in is a barrier: a closure body, a `.map`
closure, a function body, a field's VALUE, DEFAULT, ASSERT and COMPUTED
expressions and an event's THEN raise the misplaced-control-flow error, so the
FOR fails and nothing of it is kept; an event's error carries the event's name.
A BREAK in a clause with no FOR around it raises the same error as that
statement's own. ORDER BY, GROUP BY and SPLIT take only idioms, so no BREAK can
be written in them.
"""

[[test.results]]
value = "[src:1, src:2, src:3]"

[[test.results]]
value = "[ge:1]"

[[test.results]]
value = "[p11:1, p11:2, p11:3]"

[[test.results]]
value = "[p17:1, p17:2, p17:3]"

[[test.results]]
value = "[p18:1, p18:2, p18:3]"

[[test.results]]
value = "[p23:1, p23:2, p23:3]"

[[test.results]]
value = "[p24:1, p24:2, p24:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t1:11, t1:21]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[t2:11, t2:21]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p01:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p02:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p03:1, p03:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p04:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p05:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p06:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p07:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p08:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p09:1, p09:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p10:1, p10:2, p10:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 1, 0]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p12:1, p12:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p13:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p14:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p14c:1, p14c:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p15:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p16:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p16c:1, p16c:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 0, 0]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p18:2, p18:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p19:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p19c:1, p19c:3]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p20:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p21:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[p22:1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 1, 0]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 0, 0]"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "The table 'b01' does not exist"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "The table 'b02' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "The table 'b03' does not exist"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[]"

[[test.results]]
value = "[b07:1]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "The table 'b07w' does not exist"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Error while processing event e: Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."
*/

-- Setup: a source table, a relation, and tables the writes below act on.
CREATE src:1, src:2, src:3 RETURN VALUE id;
RELATE gp:1->ge:1->gp:2 RETURN VALUE id;
CREATE p11:1, p11:2, p11:3 SET k = 0 RETURN VALUE id;
CREATE p17:1, p17:2, p17:3 SET k = 0 RETURN VALUE id;
CREATE p18:1, p18:2, p18:3 SET k = 0 RETURN VALUE id;
CREATE p23:1, p23:2, p23:3 SET k = 0 RETURN VALUE id;
CREATE p24:1, p24:2, p24:3 SET k = 0 RETURN VALUE id;

-- A loop's own BREAK or CONTINUE targets that loop whatever it passes through.
FOR $o IN [1, 2] { FOR $i IN [1, 2, 3] { IF $i = 2 { BREAK }; CREATE type::record('t1', $o * 10 + $i); }; };
SELECT VALUE id FROM t1 ORDER BY id;
FOR $o IN [1, 2] { FOR $i IN [1, 2, 3] { SELECT * FROM src WHERE { IF $i = 2 { BREAK }; true }; CREATE type::record('t2', $o * 10 + $i); }; };
SELECT VALUE id FROM t2 ORDER BY id;

-- Pass: every clause written in the loop body hands BREAK and CONTINUE to the loop. The read after each loop shows the records written before the exit.
FOR $i IN [1, 2, 3] { SELECT * FROM src WHERE { IF $i = 2 { BREAK }; true }; CREATE type::record('p01', $i); };
SELECT VALUE id FROM p01 ORDER BY id;
FOR $i IN [1, 2, 3] { LET $r = (SELECT * FROM src WHERE { IF $i = 2 { BREAK }; true }); CREATE type::record('p02', $i); };
SELECT VALUE id FROM p02 ORDER BY id;
FOR $i IN [1, 2, 3] { SELECT * FROM src WHERE { IF $i = 2 { CONTINUE }; true }; CREATE type::record('p03', $i); };
SELECT VALUE id FROM p03 ORDER BY id;
FOR $i IN [1, 2, 3] { SELECT ({ IF $i = 2 { BREAK }; $i }) AS x FROM src; CREATE type::record('p04', $i); };
SELECT VALUE id FROM p04 ORDER BY id;
FOR $i IN [1, 2, 3] { SELECT * FROM src LIMIT ({ IF $i = 2 { BREAK }; 1 }); CREATE type::record('p05', $i); };
SELECT VALUE id FROM p05 ORDER BY id;
FOR $i IN [1, 2, 3] { SELECT * FROM src START ({ IF $i = 2 { BREAK }; 0 }); CREATE type::record('p06', $i); };
SELECT VALUE id FROM p06 ORDER BY id;
FOR $i IN [1, 2, 3] { LET $r = [1, 2][WHERE ({ IF $i = 2 { BREAK }; true })]; CREATE type::record('p07', $i); };
SELECT VALUE id FROM p07 ORDER BY id;
FOR $i IN [1, 2, 3] { LET $r = gp:1->ge[WHERE ({ IF $i = 2 { BREAK }; true })]->gp; CREATE type::record('p08', $i); };
SELECT VALUE id FROM p08 ORDER BY id;
FOR $i IN [1, 2, 3] { CREATE type::record('p09', $i) RETURN { IF $i = 2 { BREAK }; 1 }; };
SELECT VALUE id FROM p09 ORDER BY id;
FOR $i IN [1, 2, 3] { CREATE type::record('p10', $i) RETURN { IF $i = 2 { CONTINUE }; 1 }; };
SELECT VALUE id FROM p10 ORDER BY id;
FOR $i IN [1, 2, 3] { UPDATE type::record('p11', $i) SET k = 1 RETURN { IF $i = 2 { BREAK }; 1 }; };
SELECT VALUE k FROM p11 ORDER BY id;
FOR $i IN [1, 2, 3] { UPSERT type::record('p12', $i) SET k = 1 RETURN { IF $i = 2 { BREAK }; 1 }; };
SELECT VALUE id FROM p12 ORDER BY id;
FOR $i IN [1, 2, 3] { INSERT INTO p13 { id: $i, v: ({ IF $i = 2 { BREAK }; 1 }) }; };
SELECT VALUE id FROM p13 ORDER BY id;
FOR $i IN [1, 2, 3] { CREATE type::record('p14', $i) SET v = { IF $i = 2 { BREAK }; 1 }; };
SELECT VALUE id FROM p14 ORDER BY id;
FOR $i IN [1, 2, 3] { CREATE type::record('p14c', $i) SET v = { IF $i = 2 { CONTINUE }; 1 }; };
SELECT VALUE id FROM p14c ORDER BY id;
FOR $i IN [1, 2, 3] { CREATE type::record('p15', $i) CONTENT { v: { IF $i = 2 { BREAK }; 1 } }; };
SELECT VALUE id FROM p15 ORDER BY id;
FOR $i IN [1, 2, 3] { SELECT * FROM { IF $i = 2 { BREAK }; src:1 }; CREATE type::record('p16', $i); };
SELECT VALUE id FROM p16 ORDER BY id;
FOR $i IN [1, 2, 3] { SELECT * FROM { IF $i = 2 { CONTINUE }; src:1 }; CREATE type::record('p16c', $i); };
SELECT VALUE id FROM p16c ORDER BY id;
FOR $i IN [1, 2, 3] { UPDATE type::record('p17', $i) SET k = 1 WHERE { IF $i = 2 { BREAK }; true }; };
SELECT VALUE k FROM p17 ORDER BY id;
FOR $i IN [1, 2, 3] { DELETE type::record('p18', $i) WHERE { IF $i = 2 { BREAK }; true }; };
SELECT VALUE id FROM p18 ORDER BY id;
FOR $i IN [1, 2, 3] { LET $x = { IF $i = 2 { BREAK }; $i }; CREATE type::record('p19', $i); };
SELECT VALUE id FROM p19 ORDER BY id;
FOR $i IN [1, 2, 3] { LET $x = { IF $i = 2 { CONTINUE }; $i }; CREATE type::record('p19c', $i); };
SELECT VALUE id FROM p19c ORDER BY id;
FOR $i IN [1, 2, 3] { LET $x = (IF $i = 2 { BREAK } ELSE { $i }); CREATE type::record('p20', $i); };
SELECT VALUE id FROM p20 ORDER BY id;
FOR $i IN [1, 2, 3] { LET $x = IF $i = 2 { BREAK } ELSE { $i }; CREATE type::record('p21', $i); };
SELECT VALUE id FROM p21 ORDER BY id;
FOR $i IN [1, 2, 3] { LET $r = (SELECT * FROM src:[({ IF $i = 2 { BREAK }; $i })]); CREATE type::record('p22', $i); };
SELECT VALUE id FROM p22 ORDER BY id;
FOR $i IN [1] { UPDATE p23 SET k = 1 RETURN { IF id = p23:2 { BREAK }; 1 }; };
SELECT VALUE k FROM p23 ORDER BY id;
FOR $i IN [1] { UPDATE p24 SET k = 1 WHERE { IF id = p24:2 { BREAK }; true }; };
SELECT VALUE k FROM p24 ORDER BY id;

-- Barrier: a frame the loop is not written in raises the one misplaced-control-flow error. The FOR fails and none of its writes is kept.
FOR $i IN [1, 2, 3] { CREATE type::record('b01', $i); LET $f = |$x| { IF $x = 2 { BREAK }; $x }; LET $r = $f($i); };
SELECT VALUE id FROM b01 ORDER BY id;
FOR $i IN [1, 2, 3] { CREATE type::record('b02', $i); LET $r = [$i].map(|$x| { IF $x = 2 { BREAK }; $x }); };
SELECT VALUE id FROM b02 ORDER BY id;
DEFINE FUNCTION fn::brk($x: int) { IF $x = 2 { BREAK }; RETURN $x } PERMISSIONS FULL;
FOR $i IN [1, 2, 3] { CREATE type::record('b03', $i); LET $r = fn::brk($i); };
SELECT VALUE id FROM b03 ORDER BY id;
DEFINE TABLE b04 SCHEMALESS;
DEFINE FIELD n ON b04 VALUE { IF $value = 2 { BREAK }; $value };
FOR $i IN [1, 2, 3] { CREATE type::record('b04', $i) SET n = $i; };
SELECT VALUE id FROM b04 ORDER BY id;
DEFINE TABLE b05 SCHEMALESS;
DEFINE FIELD n ON b05 DEFAULT { IF $this.k = 2 { BREAK }; 0 };
FOR $i IN [1, 2, 3] { CREATE type::record('b05', $i) SET k = $i; };
SELECT VALUE id FROM b05 ORDER BY id;
DEFINE TABLE b06 SCHEMALESS;
DEFINE FIELD n ON b06 ASSERT { IF $value = 2 { BREAK }; true };
FOR $i IN [1, 2, 3] { CREATE type::record('b06', $i) SET n = $i; };
SELECT VALUE id FROM b06 ORDER BY id;
CREATE b07:1 SET k = 1 RETURN VALUE id;
DEFINE FIELD c ON b07 COMPUTED { BREAK };
FOR $i IN [1, 2, 3] { CREATE type::record('b07w', $i); LET $x = (SELECT * FROM b07:1); };
SELECT VALUE id FROM b07w ORDER BY id;
SELECT * FROM b07:1;
DEFINE TABLE b08 SCHEMALESS;
DEFINE EVENT e ON b08 THEN { IF $after.k = 2 { BREAK } };
FOR $i IN [1, 2, 3] { CREATE type::record('b08', $i) SET k = $i; };
SELECT VALUE id FROM b08 ORDER BY id;

-- With no loop around it, a BREAK in a clause is the statement's own error.
SELECT * FROM src WHERE { BREAK };
