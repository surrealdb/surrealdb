/**
[test]
reason = "Import: a table with a stored field the record user may write but not select, beside a field it may select."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

DEFINE TABLE actors SCHEMALESS PERMISSIONS FULL;
CREATE actors:writer;

DEFINE TABLE vault SCHEMAFULL PERMISSIONS FULL;
DEFINE FIELD secret ON vault TYPE option<string> PERMISSIONS FOR create, update FULL, FOR select NONE;
DEFINE FIELD copy ON vault TYPE option<string> PERMISSIONS FULL;
