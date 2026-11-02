/**
[test]
run = false
*/

DEFINE TABLE pg_declared SCHEMAFULL;
DEFINE FIELD v ON pg_declared TYPE string SQL TYPE POSTGRESQL 'varchar(20)';
DEFINE FIELD n ON pg_declared TYPE decimal SQL TYPE POSTGRESQL 'numeric(10,2)';
DEFINE FIELD ts ON pg_declared TYPE timestamp SQL TYPE POSTGRESQL 'timestamp(3)';
DEFINE FIELD body ON pg_declared TYPE string SQL TYPE POSTGRESQL 'text';
DEFINE FIELD u ON pg_declared TYPE uuid SQL TYPE POSTGRESQL 'uuid';
