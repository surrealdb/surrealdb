/**
[test]
reason = "Setup for 3.00.00_layout_generations.surql: the old binary writes rows of a SCHEMAFULL table, which have no layout generation."
run = false
*/
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD age ON person TYPE int;
DEFINE FIELD legacy ON person TYPE option<string>;
CREATE person:1 SET name = 'Ann', age = 41, legacy = 'kept until rewritten';
CREATE person:2 SET name = 'Bob', age = 37;
