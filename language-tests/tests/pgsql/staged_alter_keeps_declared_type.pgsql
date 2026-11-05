/**
[env]
namespace = true
database = true
imports = ["pgsql/_char5_column.surql"]

[test]
reason = "An ALTER COLUMN that changes only a column's nullability keeps the column's declared type for the statements after it in the same request"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ character_maximum_length: 5, column_name: 'code', data_type: 'character' }]"

*/
ALTER TABLE fixed ALTER COLUMN code SET NOT NULL;
SELECT column_name, data_type, character_maximum_length FROM information_schema.columns WHERE table_name = 'fixed' AND column_name = 'code';
