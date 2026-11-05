/**
[env]
namespace = true
database = true

[test]
reason = """
	Every write statement and every schema statement is refused in a schema
	clause, written in it or reached through a stored function: CREATE,
	UPDATE, UPSERT, DELETE, RELATE and INSERT, and DEFINE, REMOVE, ALTER and
	REBUILD. Each error names the clause and the statement.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The COMPUTED clause must not have effects: `CREATE` writes data"

[[test.results]]
error = "The COMPUTED clause must not have effects: `UPDATE` writes data"

[[test.results]]
error = "The COMPUTED clause must not have effects: `UPSERT` writes data"

[[test.results]]
error = "The COMPUTED clause must not have effects: `DELETE` writes data"

[[test.results]]
error = "The COMPUTED clause must not have effects: `RELATE` writes data"

[[test.results]]
error = "The COMPUTED clause must not have effects: `INSERT` writes data"

[[test.results]]
error = "The COMPUTED clause must not have effects: `DEFINE` changes the schema"

[[test.results]]
error = "The COMPUTED clause must not have effects: `REMOVE` changes the schema"

[[test.results]]
error = "The COMPUTED clause must not have effects: `ALTER` changes the schema"

[[test.results]]
error = "The COMPUTED clause must not have effects: `REBUILD` changes the schema"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The VALUE clause must not have effects: it calls `fn::upsert`, in whose body `UPSERT` writes data"

[[test.results]]
error = "The DEFAULT clause must not have effects: it calls `fn::relate`, in whose body `RELATE` writes data"

[[test.results]]
error = "The ASSERT clause must not have effects: it calls `fn::remove`, in whose body `REMOVE` changes the schema"

[[test.results]]
error = "The PERMISSIONS clause must not have effects: it calls `fn::rebuild`, in whose body `REBUILD` changes the schema"

[[test.results]]
value = "[]"

[[test.results]]
value = "['log', 't']"
*/

DEFINE TABLE log SCHEMALESS;
DEFINE INDEX ix ON log FIELDS n;
DEFINE TABLE t SCHEMALESS;

DEFINE FIELD w_create ON t COMPUTED (CREATE log RETURN NONE);
DEFINE FIELD w_update ON t COMPUTED (UPDATE log SET n = 1 RETURN NONE);
DEFINE FIELD w_upsert ON t COMPUTED (UPSERT log:1 RETURN NONE);
DEFINE FIELD w_delete ON t COMPUTED (DELETE log RETURN NONE);
DEFINE FIELD w_relate ON t COMPUTED (RELATE log:1->edge->log:2 RETURN NONE);
DEFINE FIELD w_insert ON t COMPUTED (INSERT INTO log { n: 1 } RETURN NONE);

DEFINE FIELD d_define ON t COMPUTED { DEFINE TABLE other; 1 };
DEFINE FIELD d_remove ON t COMPUTED { REMOVE TABLE log; 1 };
DEFINE FIELD d_alter ON t COMPUTED { ALTER TABLE log COMMENT 'x'; 1 };
DEFINE FIELD d_rebuild ON t COMPUTED { REBUILD INDEX ix ON log; 1 };

DEFINE FUNCTION fn::upsert() { UPSERT log:2; RETURN 1; };
DEFINE FUNCTION fn::relate() { RELATE log:1->edge->log:2; RETURN 1; };
DEFINE FUNCTION fn::remove() { REMOVE TABLE log; RETURN 1; };
DEFINE FUNCTION fn::rebuild() { REBUILD INDEX ix ON log; RETURN 1; };
DEFINE FIELD f_upsert ON t VALUE fn::upsert();
DEFINE FIELD f_relate ON t DEFAULT fn::relate();
DEFINE FIELD f_remove ON t ASSERT fn::remove() = 1;
DEFINE FIELD f_rebuild ON t PERMISSIONS FOR select WHERE fn::rebuild() = 1;

RETURN object::keys((INFO FOR TABLE t).fields);
RETURN array::sort(object::keys((INFO FOR DB).tables));
