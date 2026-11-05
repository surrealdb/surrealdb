/**
[test]
run = false
reason = "Setup only: a table with a column declared char(5), read by staged_alter_keeps_declared_type"
*/

DEFINE TABLE fixed SCHEMAFULL;
DEFINE FIELD id ON fixed TYPE int SQL TYPE POSTGRESQL 'integer';
DEFINE FIELD code ON fixed TYPE option<string> SQL TYPE POSTGRESQL 'char(5)';
