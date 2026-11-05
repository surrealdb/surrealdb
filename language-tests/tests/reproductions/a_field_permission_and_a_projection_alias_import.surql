/**
[test]
run = false
*/
DEFINE ACCESS account ON DATABASE TYPE RECORD SIGNIN ( $rid );
CREATE user:alice, user:bob;

DEFINE TABLE p SCHEMALESS PERMISSIONS FOR select FULL;
DEFINE FIELD owner ON p TYPE record<user>;
DEFINE FIELD n ON p TYPE int;
DEFINE FIELD g ON p TYPE int PERMISSIONS FOR select WHERE owner = $auth.id;

CREATE p:1 SET owner = user:alice, n = 1, g = 30;
CREATE p:2 SET owner = user:bob,   n = 2, g = 20;
CREATE p:3 SET owner = user:alice, n = 3, g = 10;
