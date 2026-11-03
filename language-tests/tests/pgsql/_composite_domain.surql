/**
[test]
run = false
reason = "Setup only: a composite type and a type over it defined in SurrealQL, which the PostgreSQL front end reports as a composite type and a domain over it, read by domain_and_attribute_of_composite_type"
*/

DEFINE TYPE point_t AS { x: none | null | i32, y: none | null | i32 } SQL TYPE POSTGRESQL '(x int, y int)';
DEFINE TYPE d_point AS point_t;
