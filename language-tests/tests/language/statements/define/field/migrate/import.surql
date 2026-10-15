/**
[test]
reason = "An OPTION IMPORT write stores a value that does not conform to its field's type, in a database at compatibility level 4 too, and the field no longer guarantees its type."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, id: p:1 }, { a: 'x', id: p:2 }]"

[[test.results]]
value = "[]"
*/
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
CREATE p:1 SET a = '1' RETURN NONE;
ALTER FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
(INFO FOR TABLE p).migrations.guaranteed;
OPTION IMPORT;
CREATE p:2 SET a = 'x';
OPTION IMPORT = false;
SELECT * FROM p ORDER BY id;
(INFO FOR TABLE p).migrations.guaranteed;
