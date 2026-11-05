/**
[env]
clean = true
remote-exclusive = true

[test]
reason = """
A `{ ... }` block given as the value of a schema statement's clause — a
`COMMENT`, a `DEFINE PARAM` or `ALTER PARAM` `VALUE`, a `DEFAULT`, a sequence's
`TIMEOUT` — is evaluated as the clause's expression before the definition is
written, and the definition holds its value. An empty block is `NONE`; a block
may bind with `LET`, write or raise, and a `BREAK` outside a loop is refused as
it is anywhere else. A clause's value is no context of its own, so a `RETURN` in
it ends the statement with its value and defines nothing.
"""

[[test.results]]
value = "NONE"

[[test.results]]
error = "The table 't' already exists"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "7"

[[test.results]]
error = "An error occurred: boom"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = """{ e: 'DEFINE TABLE e TYPE ANY SCHEMALESS PERMISSIONS NONE', t: "DEFINE TABLE t TYPE ANY SCHEMALESS COMMENT 'x' PERMISSIONS NONE", u: "DEFINE TABLE u TYPE ANY SCHEMALESS COMMENT 'yz' PERMISSIONS NONE", w: 'DEFINE TABLE w TYPE ANY SCHEMALESS PERMISSIONS NONE' }"""

[[test.results]]
value = """{ q: "DEFINE PARAM $q VALUE 3 COMMENT 'qc' PERMISSIONS FULL", r: 'DEFINE PARAM $r VALUE 5 PERMISSIONS FULL' }"""

[[test.results]]
value = """{ x: "DEFINE FUNCTION fn::x() { 1 } COMMENT 'xc' PERMISSIONS FULL" }"""

[[test.results]]
value = "{ sq: 'DEFINE SEQUENCE sq BATCH 1000 START 0 TIMEOUT 1s' }"

[[test.results]]
value = """{ f: "DEFINE FIELD f ON u DEFAULT { 1 } COMMENT 'fc' PERMISSIONS FULL" }"""

[[test.results]]
value = """"DEFINE DATABASE d COMPATIBILITY 4 COMMENT 'dc'""""

[[test.results]]
value = "'DEFINE NAMESPACE n'"

[[test.results]]
value = "[{ id: w:1 }]"
*/

DEFINE TABLE t COMMENT { 'x' };
DEFINE TABLE t COMMENT {;};
DEFINE TABLE OVERWRITE e COMMENT {;};
DEFINE NAMESPACE OVERWRITE n COMMENT {;};
DEFINE DATABASE OVERWRITE d COMMENT { 'dc' };
ALTER PARAM IF EXISTS $p VALUE {;};
DEFINE PARAM $q VALUE { 1 + 1 } COMMENT { 'qc' };
ALTER PARAM $q VALUE { 3 };
DEFINE PARAM $r VALUE { CREATE w:1; 5 };
DEFINE PARAM $s VALUE { RETURN 7; 8 };
DEFINE PARAM $u VALUE { THROW 'boom' };
DEFINE TABLE u COMMENT { LET $a = 'y'; $a + 'z' };
DEFINE FIELD f ON u DEFAULT { 1 } COMMENT { 'fc' };
DEFINE FUNCTION fn::x() { 1 } COMMENT { 'xc' };
DEFINE SEQUENCE sq TIMEOUT { 1s };
DEFINE TABLE b COMMENT { BREAK };

(INFO FOR DB).tables;
(INFO FOR DB).params;
(INFO FOR DB).functions;
(INFO FOR DB).sequences;
(INFO FOR TABLE u).fields;
(INFO FOR NS).databases.d;
(INFO FOR ROOT).namespaces.n;
SELECT * FROM w;
