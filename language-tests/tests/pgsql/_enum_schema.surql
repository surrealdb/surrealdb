/**
[test]
run = false
reason = "Setup only: a mood enum and a table of people with a mood column, read by enum_types_order"
*/

DEFINE TYPE mood AS 'sad' | 'ok' | 'happy';
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE FIELD m ON person TYPE none | null | mood;
CREATE person:1 SET name = 'a', m = 'happy' RETURN NONE;
CREATE person:2 SET name = 'b', m = 'sad' RETURN NONE;
CREATE person:3 SET name = 'c', m = 'ok' RETURN NONE;
