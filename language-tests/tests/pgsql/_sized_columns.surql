/**
[test]
run = false
*/

DEFINE TABLE sized SCHEMAFULL;
DEFINE FIELD v ON sized TYPE option<string> SQL TYPE POSTGRESQL 'varchar(20)';
DEFINE FIELD c ON sized TYPE option<string> SQL TYPE POSTGRESQL 'char(5)';
DEFINE FIELD n ON sized TYPE option<decimal> SQL TYPE POSTGRESQL 'numeric(10,2)';
DEFINE FIELD ts ON sized TYPE option<timestamp> SQL TYPE POSTGRESQL 'timestamp(3)';
