/**
[env]
namespace = true
database = true

[test]
reason = "A column's nullability follows its domain through ALTER COLUMN: a column of a domain that takes NULL still takes it once retyped, and DROP NOT NULL on a column of a NOT NULL domain does not lift the domain's NOT NULL"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: 1, x: NULL }]"

[[test.results]]
value = "[{ id: 1, x: NULL }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Couldn't coerce value for field `x` of `strict:1`: Expected `sure_int (i32)` but found `NULL`"

[[test.results]]
value = "[]"

*/

CREATE DOMAIN maybe_int AS integer;

CREATE TABLE loose (id int PRIMARY KEY, x maybe_int);

ALTER TABLE loose ALTER COLUMN x TYPE text;

INSERT INTO loose (id, x) VALUES (1, NULL);

SELECT id, x FROM loose;

CREATE DOMAIN sure_int AS integer NOT NULL;

CREATE TABLE strict (id int PRIMARY KEY, x sure_int);

ALTER TABLE strict ALTER COLUMN x DROP NOT NULL;

INSERT INTO strict (id, x) VALUES (1, NULL);

SELECT id, x FROM strict;
