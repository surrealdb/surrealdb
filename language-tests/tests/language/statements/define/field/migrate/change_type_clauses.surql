/**
[test]
reason = "Changes 1, 5, 18 and 19 together: when a statement changes a field's TYPE, a constant DEFAULT or DEFAULT ALWAYS must coerce to the new type and a VALUE or ASSERT that reads $value, kept because ALTER FIELD does not mention it, must be restated or dropped. The checks run on every statement in a database at compatibility level 4 and, at level 3, only on a statement that carries MIGRATE; a TYPE that does not change is never checked."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

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
error = "Cannot change field `a` of table `p`: its VALUE reads `$value`: restate or drop it with the new TYPE"

[[test.results]]
error = "Cannot change field `b` of table `p`: its ASSERT reads `$value`: restate or drop it with the new TYPE"

[[test.results]]
value = "'DEFINE FIELD a ON p TYPE int VALUE $value * 2 PERMISSIONS FULL'"

[[test.results]]
value = "'DEFINE FIELD b ON p TYPE int ASSERT $value > 0 PERMISSIONS FULL'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE FIELD a ON p TYPE number VALUE $value * 2 PERMISSIONS FULL'"

[[test.results]]
value = "'DEFINE FIELD b ON p TYPE number ASSERT $value > 0 PERMISSIONS FULL'"

[[test.results]]
value = "'DEFINE FIELD e ON p TYPE number VALUE 3 PERMISSIONS FULL'"

[[test.results]]
error = "Cannot change field `c` of table `p`: its DEFAULT does not coerce to `string`"

[[test.results]]
error = "Cannot change field `c` of table `p`: its DEFAULT does not coerce to `string`"

[[test.results]]
error = "Cannot change field `d` of table `p`: its DEFAULT does not coerce to `none | string`"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'DEFINE FIELD c ON p TYPE int DEFAULT 5 PERMISSIONS FULL'"

[[test.results]]
value = "11"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = """"DEFINE FIELD c ON p TYPE string DEFAULT 'x' PERMISSIONS FULL""""

[[test.results]]
value = """"DEFINE FIELD d ON p TYPE none | string DEFAULT ALWAYS 'y' PERMISSIONS FULL""""

[[test.results]]
value = "'DEFINE FIELD f ON p TYPE number DEFAULT 5 PERMISSIONS FULL'"

[[test.results]]
value = "15"

[[test.results]]
value = "NONE"

[[test.results]]
value = """"DEFINE FIELD f ON p TYPE number DEFAULT 'text' PERMISSIONS FULL""""

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change field `a` of table `e`: its VALUE reads `$value`: restate or drop it with the new TYPE"

[[test.results]]
error = "Cannot change field `c` of table `e`: its DEFAULT does not coerce to `string`"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON e TYPE int VALUE $value * 2 PERMISSIONS FULL', c: 'DEFINE FIELD c ON e TYPE int DEFAULT 5 PERMISSIONS FULL' }"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change field `d` of table `p`: its DEFAULT does not coerce to `string`"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON p TYPE number VALUE $value * 2 PERMISSIONS FULL', b: 'DEFINE FIELD b ON p TYPE number ASSERT $value > 0 PERMISSIONS FULL', c: 'DEFINE FIELD c ON p TYPE string DEFAULT 5 PERMISSIONS FULL', d: 'DEFINE FIELD d ON p TYPE int DEFAULT 5 PERMISSIONS FULL' }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Cannot change field `a` of table `p`: its VALUE reads `$value`: restate or drop it with the new TYPE"

[[test.results]]
error = "Cannot change field `b` of table `p`: its ASSERT reads `$value`: restate or drop it with the new TYPE"

[[test.results]]
error = "Cannot change field `c` of table `p`: its DEFAULT does not coerce to `string`"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON p TYPE int VALUE $value * 2 PERMISSIONS FULL', b: 'DEFINE FIELD b ON p TYPE int ASSERT $value > 0 PERMISSIONS FULL', c: 'DEFINE FIELD c ON p TYPE int DEFAULT 5 PERMISSIONS FULL', d: 'DEFINE FIELD d ON p TYPE int DEFAULT 5 PERMISSIONS FULL' }"

[[test.results]]
match = "$result.database == 'test'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON loose TYPE string DEFAULT 5 PERMISSIONS FULL', b: 'DEFINE FIELD b ON loose TYPE number VALUE $value * 2 PERMISSIONS FULL' }"
*/
-- Enforced table with rows.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int VALUE $value * 2;
DEFINE FIELD b ON p TYPE int ASSERT $value > 0;
DEFINE FIELD c ON p TYPE int DEFAULT 5;
DEFINE FIELD d ON p TYPE option<int> DEFAULT ALWAYS 5;
DEFINE FIELD e ON p TYPE int VALUE 3;
DEFINE FIELD f ON p TYPE int DEFAULT 5;
CREATE p:1 SET a = 1, b = 1, c = 1, e = 1, f = 1 RETURN NONE;
-- Even a widening is a change of TYPE.
ALTER FIELD a ON p TYPE number;
ALTER FIELD b ON p TYPE number;
(INFO FOR TABLE p).fields.a;
(INFO FOR TABLE p).fields.b;
ALTER FIELD a ON p TYPE number VALUE $value * 2;
ALTER FIELD b ON p TYPE number ASSERT $value > 0;
ALTER FIELD e ON p TYPE number;
(INFO FOR TABLE p).fields.a;
(INFO FOR TABLE p).fields.b;
(INFO FOR TABLE p).fields.e;
-- A constant DEFAULT must coerce to the new type, with or without MIGRATE.
ALTER FIELD c ON p TYPE string;
ALTER FIELD c ON p TYPE string MIGRATE EXISTING ROWS AS c_s USING <string>$value FALLBACK '';
ALTER FIELD d ON p TYPE option<string>;
ALTER FIELD f ON p TYPE number;
(INFO FOR TABLE p).fields.c;
(INFO FOR TABLE p).migrations.generation;
-- Restated, or dropped, it does.
ALTER FIELD c ON p TYPE string DEFAULT 'x' MIGRATE EXISTING ROWS AS c_s USING <string>$value FALLBACK '';
ALTER FIELD d ON p TYPE option<string> DEFAULT ALWAYS 'y' MIGRATE EXISTING ROWS AS d_s USING <string>$value FALLBACK 'z';
(INFO FOR TABLE p).fields.c;
(INFO FOR TABLE p).fields.d;
(INFO FOR TABLE p).fields.f;
(INFO FOR TABLE p).migrations.generation;
-- A TYPE that does not change is never checked.
ALTER FIELD f ON p DEFAULT 'text';
(INFO FOR TABLE p).fields.f;
ALTER FIELD f ON p DROP DEFAULT;
-- Enforced table with no rows: the checks run all the same.
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE int VALUE $value * 2;
DEFINE FIELD c ON e TYPE int DEFAULT 5;
ALTER FIELD a ON e TYPE number;
ALTER FIELD c ON e TYPE string;
(INFO FOR TABLE e).fields;
-- At compatibility level 3 only a statement with MIGRATE is checked.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE int VALUE $value * 2;
DEFINE FIELD b ON p TYPE int ASSERT $value > 0;
DEFINE FIELD c ON p TYPE int DEFAULT 5;
DEFINE FIELD d ON p TYPE int DEFAULT 5;
CREATE p:1 SET a = 1, b = 1, c = 1, d = 1 RETURN NONE;
ALTER FIELD a ON p TYPE number;
ALTER FIELD b ON p TYPE number;
ALTER FIELD c ON p TYPE string;
ALTER FIELD d ON p TYPE string MIGRATE EXISTING ROWS AS d_s USING <string>$value FALLBACK '';
(INFO FOR TABLE p).fields;
-- With MIGRATE, a field whose TYPE changes is checked.
DEFINE FIELD OVERWRITE a ON p TYPE int VALUE $value * 2;
DEFINE FIELD OVERWRITE b ON p TYPE int ASSERT $value > 0;
DEFINE FIELD OVERWRITE c ON p TYPE int DEFAULT 5;
ALTER FIELD a ON p TYPE string MIGRATE EXISTING ROWS AS a_s USING <string>$value FALLBACK '';
ALTER FIELD b ON p TYPE string MIGRATE EXISTING ROWS AS b_s USING <string>$value FALLBACK '';
ALTER FIELD c ON p TYPE string MIGRATE EXISTING ROWS AS c_s USING <string>$value FALLBACK '';
(INFO FOR TABLE p).fields;
-- A SCHEMALESS table at level 4 is not enforced.
USE DB test;
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD a ON loose TYPE int DEFAULT 5;
DEFINE FIELD b ON loose TYPE int VALUE $value * 2;
ALTER FIELD a ON loose TYPE string;
ALTER FIELD b ON loose TYPE number;
(INFO FOR TABLE loose).fields;
