/**
[test]
run = false
reason = "Setup for range_operators: a table whose fields declare int4range, daterange and int4multirange, so a request that holds no DDL lowers against their declared types"
*/

DEFINE TABLE booking SCHEMAFULL;
DEFINE FIELD seats ON booking TYPE option<range> SQL TYPE POSTGRESQL 'int4range';
DEFINE FIELD days ON booking TYPE option<range> SQL TYPE POSTGRESQL 'daterange';
DEFINE FIELD spans ON booking TYPE option<array<range>> SQL TYPE POSTGRESQL 'int4multirange';
