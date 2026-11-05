/**
[test]
run = false
reason = "Setup for sql_types_geometry_year_bit: a table whose fields declare MySQL YEAR, BIT(4) and POINT SRID 4326, with one record"
*/

DEFINE TABLE place SCHEMAFULL;
DEFINE FIELD built ON place TYPE option<int> SQL TYPE MYSQL 'year';
DEFINE FIELD flags ON place TYPE option<int> SQL TYPE MYSQL 'bit(4)';
DEFINE FIELD spot ON place TYPE option<geometry<point>> SQL TYPE MYSQL 'point srid 4326';

CREATE place:1 SET built = 2024, flags = 5, spot = (1.5, 2.5) RETURN NONE;
