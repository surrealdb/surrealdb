/**
[test]
reason = "Setup for return_none_pruned_gate.surql: twin tables under each select clause the stages can prove inert (FULL, NONE, and a predicate over the session alone), each with an edge table and views that show the stored rows, so a RETURN NONE write and a RETURN AFTER write of the same statements can be compared."
run = false
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:alice;
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE TABLE pf_a SCHEMALESS PERMISSIONS FOR select FULL FOR create, update, delete FULL;
DEFINE TABLE pf_a_e SCHEMALESS PERMISSIONS FOR select FULL FOR create, update, delete FULL;
DEFINE TABLE pf_a_state AS SELECT id, n FROM pf_a PERMISSIONS FULL;
DEFINE TABLE pf_a_e_state AS SELECT id, in, out, n FROM pf_a_e PERMISSIONS FULL;
DEFINE TABLE pf_b SCHEMALESS PERMISSIONS FOR select FULL FOR create, update, delete FULL;
DEFINE TABLE pf_b_e SCHEMALESS PERMISSIONS FOR select FULL FOR create, update, delete FULL;
DEFINE TABLE pf_b_state AS SELECT id, n FROM pf_b PERMISSIONS FULL;
DEFINE TABLE pf_b_e_state AS SELECT id, in, out, n FROM pf_b_e PERMISSIONS FULL;

DEFINE TABLE pn_a SCHEMALESS PERMISSIONS FOR select NONE FOR create, update, delete FULL;
DEFINE TABLE pn_a_e SCHEMALESS PERMISSIONS FOR select NONE FOR create, update, delete FULL;
DEFINE TABLE pn_a_state AS SELECT id, n FROM pn_a PERMISSIONS FULL;
DEFINE TABLE pn_a_e_state AS SELECT id, in, out, n FROM pn_a_e PERMISSIONS FULL;
DEFINE TABLE pn_b SCHEMALESS PERMISSIONS FOR select NONE FOR create, update, delete FULL;
DEFINE TABLE pn_b_e SCHEMALESS PERMISSIONS FOR select NONE FOR create, update, delete FULL;
DEFINE TABLE pn_b_state AS SELECT id, n FROM pn_b PERMISSIONS FULL;
DEFINE TABLE pn_b_e_state AS SELECT id, in, out, n FROM pn_b_e PERMISSIONS FULL;

DEFINE TABLE pa_a SCHEMALESS PERMISSIONS FOR select WHERE $auth != NONE FOR create, update, delete FULL;
DEFINE TABLE pa_a_e SCHEMALESS PERMISSIONS FOR select WHERE $auth != NONE FOR create, update, delete FULL;
DEFINE TABLE pa_a_state AS SELECT id, n FROM pa_a PERMISSIONS FULL;
DEFINE TABLE pa_a_e_state AS SELECT id, in, out, n FROM pa_a_e PERMISSIONS FULL;
DEFINE TABLE pa_b SCHEMALESS PERMISSIONS FOR select WHERE $auth != NONE FOR create, update, delete FULL;
DEFINE TABLE pa_b_e SCHEMALESS PERMISSIONS FOR select WHERE $auth != NONE FOR create, update, delete FULL;
DEFINE TABLE pa_b_state AS SELECT id, n FROM pa_b PERMISSIONS FULL;
DEFINE TABLE pa_b_e_state AS SELECT id, in, out, n FROM pa_b_e PERMISSIONS FULL;

