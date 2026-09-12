/**
[test]
reason = "Setup: a table the record user may not read at all, one whose select predicate no record satisfies, and one it may read in full — each with an index so the indexed count shape is reachable."
run = false
*/

DEFINE TABLE denied SCHEMALESS PERMISSIONS NONE;
DEFINE TABLE guarded SCHEMALESS PERMISSIONS FOR select WHERE false;
DEFINE TABLE shown SCHEMALESS PERMISSIONS FULL;
DEFINE INDEX denied_k ON denied FIELDS k;
DEFINE INDEX guarded_k ON guarded FIELDS k;
DEFINE INDEX shown_k ON shown FIELDS k;
DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN (SELECT * FROM user) DURATION FOR SESSION 1h;
CREATE user:1;
CREATE denied:1 SET k = 1;
CREATE guarded:1 SET k = 1;
CREATE shown:1 SET k = 1;
