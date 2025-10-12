/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "'users'"

[[test.results]]
error = "Tried to set `$table_bad`, but couldn't coerce value: Expected `table<users>` but found `'posts'`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'posts'"

*/

-- Define a typed variable with a specific table
LET $table_users: table<users> = 'users';
RETURN $table_users;

-- Try to set a variable with wrong table name (should fail)
LET $table_bad: table<users> = 'posts';

-- Define a variable with union of table types
LET $table_union: table<users | posts> = 'posts';
RETURN $table_union;

