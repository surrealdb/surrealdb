/**
[test]
reason = "Setup: tables whose select permissions read a filtered lookup or a destructure, and computed fields holding a method and an element filter beside a field permission."
run = false
*/
DEFINE ACCESS account ON DATABASE TYPE RECORD SIGNIN ( SELECT * FROM type::record('user', $user) ) DURATION FOR TOKEN 18m, FOR SESSION 12h;
DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE owned_by SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE cu SCHEMALESS PERMISSIONS FULL;
CREATE user:alice SET active = true;
CREATE user:bob SET active = false;
DEFINE TABLE doc SCHEMALESS PERMISSIONS FOR select WHERE ->owned_by->(user WHERE id = $auth.id);
CREATE doc:1, doc:2;
RELATE doc:1->owned_by->user:alice;
RELATE doc:2->owned_by->user:bob;
DEFINE TABLE pd SCHEMALESS PERMISSIONS FOR select WHERE $this.{owner}.owner = $auth.id;
CREATE pd:1 SET owner = user:alice;
CREATE pd:2 SET owner = user:bob;
DEFINE FIELD s ON cu TYPE option<string> PERMISSIONS FOR select WHERE $auth.id = user:bob;
DEFINE FIELD n ON cu COMPUTED tags.len();
DEFINE FIELD m ON cu COMPUTED tags[WHERE $this > 1];
CREATE cu:1 SET tags = [1, 2, 3], s = 'x';
