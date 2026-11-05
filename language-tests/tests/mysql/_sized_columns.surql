/**
[test]
run = false
*/

DEFINE TABLE sized SCHEMAFULL;
DEFINE FIELD v ON sized TYPE option<string> SQL TYPE MYSQL 'VARCHAR(20)';
DEFINE FIELD c ON sized TYPE option<string> SQL TYPE MYSQL 'CHAR(5)';
DEFINE FIELD n ON sized TYPE option<decimal> SQL TYPE MYSQL 'NUMERIC(10,2)';
DEFINE FIELD ts ON sized TYPE option<datetime> SQL TYPE MYSQL 'TIMESTAMP(3)';

DEFINE TABLE ytypes SCHEMAFULL;
DEFINE FIELD y ON ytypes TYPE option<int> SQL TYPE MYSQL 'YEAR';
DEFINE FIELD b ON ytypes TYPE option<int> SQL TYPE MYSQL 'BIT(8)';
DEFINE FIELD d ON ytypes TYPE option<decimal> SQL TYPE MYSQL 'DECIMAL(10,2)';
DEFINE FIELD dt ON ytypes TYPE option<datetime> SQL TYPE MYSQL 'DATETIME(3)';
