/**
[test]
reason = "Import: a table with a field hidden from the writer via FOR select NONE. Used to verify data-clause expressions evaluate against the reduced view, not the raw doc."
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

DEFINE TABLE actors SCHEMALESS PERMISSIONS FULL;
CREATE actors:writer;

DEFINE TABLE vault SCHEMAFULL PERMISSIONS FOR select, create, update FULL, FOR delete NONE;

-- The writer can read `copy` and write any field, but cannot select
-- `secret`. A SET/MERGE/CONTENT/PATCH expression that copies `secret`
-- into another field must see `secret = NONE` under the reduced view.
DEFINE FIELD secret ON vault TYPE option<string> PERMISSIONS FOR create, update FULL, FOR select NONE;

DEFINE FIELD copy ON vault TYPE option<string> PERMISSIONS FULL;

CREATE vault:1 SET secret = 'classified', copy = NONE;
