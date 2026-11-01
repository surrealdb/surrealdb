/**
[test]
run = false
reason = "Setup for omitted_column_is_null: a schemafull table declared as CREATE TABLE declares one, with columns that hold NULL and one with a default"
*/

DEFINE TABLE omit_t SCHEMAFULL;
DEFINE FIELD a ON omit_t TYPE none | null | int;
DEFINE FIELD b ON omit_t TYPE none | null | string;
DEFINE FIELD d ON omit_t TYPE none | null | int DEFAULT 7;
