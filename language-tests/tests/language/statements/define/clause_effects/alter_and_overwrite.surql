/**
[env]
namespace = true
database = true

[test]
reason = """
	ALTER and DEFINE ... OVERWRITE are refused as DEFINE is when a clause they
	write has an effect a schema clause must not have, and the definition is
	left as it was. An ALTER judges only the clauses it changes.
"""

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
error = "The VALUE clause must not have effects: it calls `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
error = "The ASSERT clause must not have effects: `CREATE` writes data"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `sequence::nextval` advances state outside the transaction"

[[test.results]]
error = "The DEFAULT clause must not have effects: `SLEEP` blocks"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: it calls `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `sequence::nextval` advances state outside the transaction"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: it calls `fn::writes`, in whose body `CREATE` writes data"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: `DEFINE` changes the schema"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """{ f: "DEFINE FIELD f ON t VALUE 1 COMMENT 'kept' PERMISSIONS FULL" }"""

[[test.results]]
value = "'DEFINE TABLE t TYPE ANY SCHEMALESS PERMISSIONS FOR select WHERE fn::reads(), FOR create, update, delete NONE'"

[[test.results]]
value = "[{ count: 0 }]"
*/

DEFINE SEQUENCE s;
DEFINE TABLE log SCHEMALESS;
DEFINE FUNCTION fn::writes() { CREATE log; RETURN true; };
DEFINE FUNCTION fn::reads() { RETURN true; };
DEFINE TABLE t SCHEMALESS;
DEFINE FIELD f ON t VALUE 1;

ALTER FIELD f ON t VALUE fn::writes();
ALTER FIELD f ON t ASSERT (CREATE log RETURN NONE) = NONE;
ALTER FIELD f ON t PERMISSIONS FOR select WHERE sequence::nextval('s') > 0;
DEFINE FIELD OVERWRITE f ON t DEFAULT { SLEEP 1ms; 1 };
ALTER TABLE t PERMISSIONS FOR select WHERE fn::writes();
DEFINE TABLE OVERWRITE t PERMISSIONS FOR delete WHERE sequence::nextval('s') > 0;
DEFINE FUNCTION OVERWRITE fn::reads() { RETURN true; } PERMISSIONS WHERE fn::writes();
DEFINE PARAM OVERWRITE $p VALUE 1 PERMISSIONS WHERE { DEFINE TABLE other; true };

ALTER FIELD f ON t COMMENT 'kept';
ALTER TABLE t PERMISSIONS FOR select WHERE fn::reads();

(INFO FOR TABLE t).fields;
(INFO FOR DB).tables.t;
SELECT count() FROM log GROUP ALL;
