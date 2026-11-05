/**
[test]
reason = "Change 5 on an array field: a TYPE change of a field whose kind implies an element definition (tags.*) leaves the definitions beneath it fitting the new TYPE, so a row written after the change may hold elements of the new element type."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: p:1, tags: [1, 2], ti: true }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: p:2, tags: [3, 4], ti: true }]"

[[test.results]]
value = "NONE"

[[test.results]]
match = "$result.database == 'd3'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ id: p:1, tags: [3, 4], ti: true }]"
*/
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD tags ON p TYPE array<string>;
CREATE p:1 SET tags = ['1', '2'] RETURN NONE;
ALTER FIELD tags ON p TYPE array<int> MIGRATE EXISTING ROWS AS tags_int USING <array<int>>$value FALLBACK [];
SELECT id, tags, type::is_int(tags[0]) AS ti FROM p;
CREATE p:2 SET tags = [3, 4] RETURN NONE;
SELECT id, tags, type::is_int(tags[0]) AS ti FROM p:2;
-- A table that is not enforced is no different.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD tags ON p TYPE array<string>;
ALTER FIELD tags ON p TYPE array<int>;
CREATE p:1 SET tags = [3, 4] RETURN NONE;
SELECT id, tags, type::is_int(tags[0]) AS ti FROM p:1;
