/**
[test]
run = false
reason = "Setup only: the table CREATE TABLE jt (id INT PRIMARY KEY, j JSON) defines, read by declared_type_json_columns"
*/

DEFINE TABLE jt SCHEMAFULL;
DEFINE FIELD id ON jt TYPE int SQL TYPE MYSQL 'INT';
DEFINE FIELD j ON jt TYPE any SQL TYPE MYSQL 'JSON';
