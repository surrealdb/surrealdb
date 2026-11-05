/**
[test]
reason = "Setup for return_none_select_gate_raises.surql: tables whose select permission raises on one record (a cast that fails, a computed field whose body raises, a computed field whose declared kind its infallible body does not meet), and a view that shows the stored rows of the first."
run = false
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:alice;
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE cast_gate SCHEMALESS
	PERMISSIONS FOR select WHERE <int> code > 0 FOR create, update, delete FULL;
DEFINE TABLE cast_gate_state AS SELECT id, code, touched FROM cast_gate PERMISSIONS FULL;
CREATE cast_gate:1 SET code = 1;
CREATE cast_gate:2 SET code = 'x';
CREATE cast_gate:3 SET code = 3;

-- A table with a computed field has no view: a view observes the record, which derives the
-- field for it.
DEFINE TABLE derived_gate SCHEMALESS
	PERMISSIONS FOR select WHERE total > 0 FOR create, update, delete FULL;
DEFINE FIELD total ON derived_gate COMPUTED <int> code + 1;
CREATE derived_gate:1 SET code = 1 RETURN NONE;
CREATE derived_gate:2 SET code = 'x' RETURN NONE;
CREATE derived_gate:3 SET code = 3 RETURN NONE;

DEFINE TABLE typed_gate SCHEMALESS
	PERMISSIONS FOR select WHERE total > 0 FOR create, update, delete FULL;
DEFINE FIELD total ON typed_gate COMPUTED code TYPE int;
CREATE typed_gate:1 SET code = 1 RETURN NONE;
CREATE typed_gate:3 SET code = 3 RETURN NONE;
