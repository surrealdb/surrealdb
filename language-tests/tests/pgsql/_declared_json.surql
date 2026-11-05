/**
[test]
run = false
reason = "Setup only: the table CREATE TABLE jt (id INT PRIMARY KEY, j JSON, jb JSONB) defines, read by declared_type_json_columns"
*/

DEFINE TABLE jt SCHEMAFULL;
DEFINE FIELD id ON jt TYPE int SQL TYPE POSTGRESQL 'integer';
DEFINE FIELD j ON jt TYPE any SQL TYPE POSTGRESQL 'json';
DEFINE FIELD jb ON jt TYPE any SQL TYPE POSTGRESQL 'jsonb';
