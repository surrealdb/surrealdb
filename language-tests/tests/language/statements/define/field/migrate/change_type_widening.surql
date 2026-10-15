/**
[test]
reason = "Changes 1-4: widening a field's TYPE (int to number, T to option<T>, T to T | U, adding FLEXIBLE) is accepted by every table with or without rows, keeps a Guaranteed field Guaranteed, mints no generation, and leaves old rows reading the same values; a later write may hold the wider values."

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
value = "NONE"

[[test.results]]
value = "12"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ a: 1, ai: true, b: 2, bi: true, c: 3, ci: true, o: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON p TYPE number PERMISSIONS FULL', b: 'DEFINE FIELD b ON p TYPE none | int PERMISSIONS FULL', c: 'DEFINE FIELD c ON p TYPE int | string PERMISSIONS FULL', o: 'DEFINE FIELD o ON p TYPE object FLEXIBLE PERMISSIONS FULL' }"

[[test.results]]
value = "15"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ a: 1, ai: true, b: 2, bi: true, c: 3, ci: true, o: {  } }"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ a: 2.5f, af: true, b: NONE, c: 'x', cs: true, o: { k: 'v' } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "16"

[[test.results]]
value = "[]"

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
value = "7"

[[test.results]]
value = "[]"

[[test.results]]
value = "[{ a: 8f, af: true, id: s:1 }, { a: 10f, af: true, id: s:2 }, { a: 13f, af: true, id: s:3 }]"

[[test.results]]
value = "[10f, 13f]"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON e TYPE number PERMISSIONS FULL', b: 'DEFINE FIELD b ON e TYPE none | int PERMISSIONS FULL', c: 'DEFINE FIELD c ON e TYPE int | string PERMISSIONS FULL', o: 'DEFINE FIELD o ON e TYPE object FLEXIBLE PERMISSIONS FULL' }"

[[test.results]]
value = "8"

[[test.results]]
value = "[]"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ a: 1, ai: true, b: 2, bi: true, c: 3, ci: true, o: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ database: 'd3', namespace: 'test' }"

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
value = "15"

[[test.results]]
value = "[]"

[[test.results]]
value = "{ a: 1, ai: true, b: 2, bi: true, c: 3, ci: true, o: {  } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: 'DEFINE FIELD a ON e TYPE number PERMISSIONS FULL' }"

[[test.results]]
value = "NONE"
*/
-- Enforced table with rows: a migration makes each field Guaranteed first,
-- so a widening has a state to keep.
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
DEFINE FIELD b ON p TYPE string;
DEFINE FIELD c ON p TYPE string;
DEFINE FIELD o ON p TYPE object;
CREATE p:1 SET a = '1', b = '2', c = '3', o = {} RETURN NONE;
ALTER FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD b ON p TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
ALTER FIELD c ON p TYPE int MIGRATE EXISTING ROWS AS c_int USING <int>$value FALLBACK 0;
ALTER FIELD o ON p MIGRATE EXISTING ROWS AS o_obj USING $value FALLBACK {};
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT a, b, c, o, type::is_int(a) AS ai, type::is_int(b) AS bi, type::is_int(c) AS ci FROM ONLY p:1;
-- 1 int to number, 2 T to option<T>, 3 T to T | U, 4 adding FLEXIBLE.
ALTER FIELD a ON p TYPE number;
ALTER FIELD b ON p TYPE option<int>;
ALTER FIELD c ON p TYPE int | string;
ALTER FIELD o ON p FLEXIBLE;
(INFO FOR TABLE p).fields;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT a, b, c, o, type::is_int(a) AS ai, type::is_int(b) AS bi, type::is_int(c) AS ci FROM ONLY p:1;
-- A row written afterwards may hold what the wider definitions accept.
CREATE p:2 SET a = 2.5, b = NONE, c = 'x', o = { k: 'v' } RETURN NONE;
SELECT a, b, c, o, type::is_float(a) AS af, type::is_string(c) AS cs FROM ONLY p:2;
-- The same widenings through DEFINE FIELD OVERWRITE.
DEFINE FIELD OVERWRITE a ON p TYPE number | string;
DEFINE FIELD OVERWRITE b ON p TYPE option<int> | string;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
-- A widening between two steps mints nothing and changes neither: a row
-- applies exactly the steps it missed.
DEFINE TABLE s SCHEMAFULL;
DEFINE FIELD a ON s TYPE string;
CREATE s:1 SET a = '4' RETURN NONE;
ALTER FIELD a ON s TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
CREATE s:2 SET a = 5 RETURN NONE;
ALTER FIELD a ON s TYPE number;
CREATE s:3 SET a = 6.5 RETURN NONE;
ALTER FIELD a ON s TYPE float MIGRATE EXISTING ROWS AS a_float USING $value * 2 FALLBACK 0;
(INFO FOR TABLE s).migrations.generation;
(INFO FOR TABLE s).migrations.guaranteed;
SELECT id, a, type::is_float(a) AS af FROM s ORDER BY id;
SELECT VALUE a FROM s WHERE a > 8 ORDER BY id;
-- A SCHEMAFULL table with no rows has nothing to keep: it accepts the same
-- statements and mints nothing.
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE int;
DEFINE FIELD b ON e TYPE int;
DEFINE FIELD c ON e TYPE int;
DEFINE FIELD o ON e TYPE object;
ALTER FIELD a ON e TYPE number;
ALTER FIELD b ON e TYPE option<int>;
ALTER FIELD c ON e TYPE int | string;
ALTER FIELD o ON e FLEXIBLE;
(INFO FOR TABLE e).fields;
(INFO FOR TABLE e).migrations.generation;
(INFO FOR TABLE e).migrations.guaranteed;
-- A SCHEMALESS table is not enforced, and its widenings keep the state too
-- (FLEXIBLE is a SCHEMAFULL clause).
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD a ON loose TYPE string;
DEFINE FIELD b ON loose TYPE string;
DEFINE FIELD c ON loose TYPE string;
DEFINE FIELD o ON loose TYPE object;
CREATE loose:1 SET a = '1', b = '2', c = '3', o = {} RETURN NONE;
ALTER FIELD a ON loose TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD b ON loose TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
ALTER FIELD c ON loose TYPE int MIGRATE EXISTING ROWS AS c_int USING <int>$value FALLBACK 0;
ALTER FIELD o ON loose MIGRATE EXISTING ROWS AS o_obj USING $value FALLBACK {};
ALTER FIELD a ON loose TYPE number;
ALTER FIELD b ON loose TYPE option<int>;
ALTER FIELD c ON loose TYPE int | string;
(INFO FOR TABLE loose).migrations.generation;
(INFO FOR TABLE loose).migrations.guaranteed;
SELECT a, b, c, o, type::is_int(a) AS ai, type::is_int(b) AS bi, type::is_int(c) AS ci FROM ONLY loose:1;
-- A database at compatibility level 3: no table is enforced, and a widening
-- behaves the same.
DEFINE DATABASE d3 COMPATIBILITY 3;
USE DB d3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE string;
DEFINE FIELD b ON p TYPE string;
DEFINE FIELD c ON p TYPE string;
DEFINE FIELD o ON p TYPE object;
CREATE p:1 SET a = '1', b = '2', c = '3', o = {} RETURN NONE;
ALTER FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
ALTER FIELD b ON p TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
ALTER FIELD c ON p TYPE int MIGRATE EXISTING ROWS AS c_int USING <int>$value FALLBACK 0;
ALTER FIELD o ON p MIGRATE EXISTING ROWS AS o_obj USING $value FALLBACK {};
ALTER FIELD a ON p TYPE number;
ALTER FIELD b ON p TYPE option<int>;
ALTER FIELD c ON p TYPE int | string;
ALTER FIELD o ON p FLEXIBLE;
(INFO FOR TABLE p).migrations.generation;
(INFO FOR TABLE p).migrations.guaranteed;
SELECT a, b, c, o, type::is_int(a) AS ai, type::is_int(b) AS bi, type::is_int(c) AS ci FROM ONLY p:1;
DEFINE TABLE e SCHEMAFULL;
DEFINE FIELD a ON e TYPE int;
ALTER FIELD a ON e TYPE number;
(INFO FOR TABLE e).fields;
(INFO FOR TABLE e).migrations;
