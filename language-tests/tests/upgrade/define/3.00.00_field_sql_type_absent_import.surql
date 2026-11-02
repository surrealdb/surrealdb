/**
[test]
reason = """
Setup for 3.00.00_field_sql_type_absent.surql: the old binary defines a table with
`string` and `decimal` fields, which carry no declared SQL type, and writes a row."""
run = false
*/
DEFINE TABLE person TYPE NORMAL SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD price ON person TYPE decimal;
CREATE person:1 SET name = 'alexander', price = 1.005dec;
