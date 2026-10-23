/**
[test]
wip = true
reason = "Not built: a non-constant DEFAULT is not checked against the new TYPE by its inferred type. Changes 5 and 19 together: when a statement changes a field's TYPE, a DEFAULT or DEFAULT ALWAYS that is not a constant must have an inferred type that can overlap the new TYPE, as a constant one must coerce to it. A DEFAULT that can never produce a value of the new type is refused, whatever the wording of the refusal, and the definition is left as it was."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = true

[[test.results]]
error = true

[[test.results]]
value = "'DEFINE FIELD t ON p TYPE datetime DEFAULT time::now() PERMISSIONS FULL'"

[[test.results]]
value = "'DEFINE FIELD u ON p TYPE string DEFAULT ALWAYS `rand`::string(3) PERMISSIONS FULL'"

[[test.results]]
value = "0"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE FIELD k ON p TYPE number DEFAULT `rand`::int() PERMISSIONS FULL'"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'd3'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
error = true

[[test.results]]
value = "'DEFINE FIELD t ON p TYPE datetime DEFAULT time::now() PERMISSIONS FULL'"
*/
-- Enforced table with rows.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD t ON p TYPE datetime DEFAULT time::now();
DEFINE FIELD u ON p TYPE string DEFAULT ALWAYS rand::string(3);
DEFINE FIELD k ON p TYPE int DEFAULT rand::int();
CREATE p:1 SET t = time::now(), u = 'u', k = 1 RETURN NONE;
-- Their inferred types are datetime and string: neither can be an int.
ALTER FIELD t ON p TYPE int MIGRATE EXISTING ROWS AS t_int USING 1 FALLBACK 0;
ALTER FIELD u ON p TYPE int MIGRATE EXISTING ROWS AS u_int USING 1 FALLBACK 0;
(INFO FOR TABLE p).fields.t;
(INFO FOR TABLE p).fields.u;
(INFO FOR TABLE p).migrations.generation;
-- An int default can be a number.
ALTER FIELD k ON p TYPE number;
(INFO FOR TABLE p).fields.k;
-- At compatibility level 3 a statement with MIGRATE is checked as well.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD t ON p TYPE datetime DEFAULT time::now();
CREATE p:1 SET t = time::now() RETURN NONE;
ALTER FIELD t ON p TYPE int MIGRATE EXISTING ROWS AS t_int USING 1 FALLBACK 0;
(INFO FOR TABLE p).fields.t;
