/**
[test]
reason = "Setup: a record-user table whose select, update and delete permissions admit the owner's rows, with a brute-force table and an HNSW table of the same rows. The hidden row t:2 is the nearest record to the query vector."
run = false
*/
DEFINE ACCESS account ON DATABASE TYPE RECORD SIGNIN ( SELECT * FROM type::record('user', $user) ) DURATION FOR TOKEN 18m, FOR SESSION 12h;
DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
CREATE user:alice;
CREATE user:bob;
DEFINE TABLE t SCHEMALESS PERMISSIONS FOR select, update, delete WHERE owner = $auth.id;
CREATE t:1 SET owner = user:alice, v = [1.0, 0.0, 0.0];
CREATE t:2 SET owner = user:bob, v = [0.1, 0.0, 0.0];
CREATE t:3 SET owner = user:alice, v = [5.0, 5.0, 5.0];
CREATE t:4 SET owner = user:bob, v = [1.0, 1.0, 1.0];
DEFINE TABLE t_h SCHEMALESS PERMISSIONS FOR select, update, delete WHERE owner = $auth.id;
CREATE t_h:1 SET owner = user:alice, v = [1.0, 0.0, 0.0];
CREATE t_h:2 SET owner = user:bob, v = [0.1, 0.0, 0.0];
CREATE t_h:3 SET owner = user:alice, v = [5.0, 5.0, 5.0];
CREATE t_h:4 SET owner = user:bob, v = [1.0, 1.0, 1.0];
DEFINE INDEX t_h_v ON t_h FIELDS v HNSW DIMENSION 3 DIST EUCLIDEAN;
