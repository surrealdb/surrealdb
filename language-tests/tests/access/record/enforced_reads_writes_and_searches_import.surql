/**
[test]
reason = "Setup: a table whose select and update permissions admit the owner's rows, with a vector index."
run = false
*/
DEFINE ACCESS account ON DATABASE TYPE RECORD SIGNIN ( SELECT * FROM type::record('user', $user) ) DURATION FOR TOKEN 18m, FOR SESSION 12h;
DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
CREATE user:alice;
DEFINE TABLE t SCHEMALESS PERMISSIONS FOR select, update WHERE owner = $auth.id;
CREATE t:1 SET owner = user:alice, v = [1.0, 2.0];
CREATE t:2 SET owner = user:bob, v = [1.0, 2.1];
DEFINE INDEX hv ON t FIELDS v HNSW DIMENSION 2;
