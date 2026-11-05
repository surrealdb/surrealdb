/**
[test]
reason = """
	Setup for clause_effects/runtime. One schema clause of each kind calls a
	stored function that reads when the clause is defined, so every definition
	is accepted; each function is then redefined to have an effect a schema
	clause must not have. The test that imports this reads and writes as a
	record user, so every clause runs, and asserts each is refused where it
	runs.
"""
run = false
*/

DEFINE TABLE log SCHEMALESS PERMISSIONS FULL;
DEFINE SEQUENCE s;

DEFINE FUNCTION fn::sel() { RETURN true; } PERMISSIONS FULL;
DEFINE FUNCTION fn::upd() { RETURN true; } PERMISSIONS FULL;
DEFINE FUNCTION fn::comp() { RETURN 1; } PERMISSIONS FULL;
DEFINE FUNCTION fn::val() { RETURN 1; } PERMISSIONS FULL;
DEFINE FUNCTION fn::def() { RETURN 1; } PERMISSIONS FULL;
DEFINE FUNCTION fn::ass() { RETURN true; } PERMISSIONS FULL;
DEFINE FUNCTION fn::fperm() { RETURN true; } PERMISSIONS FULL;
DEFINE FUNCTION fn::parp() { RETURN true; } PERMISSIONS FULL;

DEFINE TABLE tsel SCHEMALESS PERMISSIONS FOR select WHERE fn::sel();
DEFINE TABLE tupd SCHEMALESS PERMISSIONS FOR select FULL FOR create, update WHERE fn::upd();
DEFINE TABLE tfld SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD c ON tfld COMPUTED fn::comp();
DEFINE TABLE tval SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD v ON tval VALUE fn::val();
DEFINE TABLE tdef SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD d ON tdef DEFAULT fn::def();
DEFINE TABLE tass SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD a ON tass ASSERT fn::ass();
DEFINE TABLE tfp SCHEMALESS PERMISSIONS FULL;
DEFINE FIELD f ON tfp PERMISSIONS FOR select FULL FOR create, update WHERE fn::fperm();
DEFINE PARAM $guarded VALUE 1 PERMISSIONS WHERE fn::parp();

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:test;
CREATE tsel:1;
CREATE tfld:1;

-- Each callee is redefined after the clause that calls it, to an effect.
DEFINE FUNCTION OVERWRITE fn::sel() { CREATE log:sel; RETURN true; } PERMISSIONS FULL;
DEFINE FUNCTION OVERWRITE fn::upd() { RETURN sequence::nextval('s') > 0; } PERMISSIONS FULL;
DEFINE FUNCTION OVERWRITE fn::comp() { CREATE log:comp; RETURN 1; } PERMISSIONS FULL;
DEFINE FUNCTION OVERWRITE fn::val() { SLEEP 1ms; RETURN 1; } PERMISSIONS FULL;
DEFINE FUNCTION OVERWRITE fn::def() { DEFINE TABLE made; RETURN 1; } PERMISSIONS FULL;
DEFINE FUNCTION OVERWRITE fn::ass() { UPSERT log:ass; RETURN true; } PERMISSIONS FULL;
DEFINE FUNCTION OVERWRITE fn::fperm() { INSERT INTO log { id: log:fperm }; RETURN true; } PERMISSIONS FULL;
DEFINE FUNCTION OVERWRITE fn::parp() { UPSERT log:parp; RETURN true; } PERMISSIONS FULL;
