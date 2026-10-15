-- A SCHEMALESS table takes non-widening retypes without MIGRATE and reads its
-- stored rows as they are; a MIGRATE on it is applied to the stored rows like any
-- other, and writes after the change are checked against the new TYPE. A
-- database at compatibility level 3 inside the same scenario accepts the same
-- retypes and must answer the same rows; a plain retype there mints no
-- generation. Probes a NULL row and a non-numeric row under a MIGRATE with a
-- FALLBACK, and a no-FALLBACK cast to float once the source type is guaranteed.
DEFINE TABLE sl SCHEMALESS;
DEFINE FIELD a ON sl TYPE any;
DEFINE FIELD b ON sl TYPE any;
CREATE sl:1 SET a = 1, b = 'x';
CREATE sl:2 SET a = 'two', b = 2;
CREATE sl:3 SET a = 3.5, b = 'y';
CREATE sl:4 SET b = 'z';
CREATE sl:5 SET a = NULL, b = [1];
SELECT VALUE a FROM sl ORDER BY id;
SELECT VALUE b FROM sl ORDER BY id;
-- @upgrade
ALTER FIELD a ON sl TYPE string;
ALTER FIELD b ON sl TYPE int MIGRATE EXISTING ROWS AS b_int USING <int>$value FALLBACK 0;
SELECT VALUE a FROM sl ORDER BY id;
SELECT VALUE b FROM sl ORDER BY id;
DEFINE INDEX by_a ON sl FIELDS a;
SELECT VALUE b FROM sl WHERE b = 0 ORDER BY id;
-- @check
-- @drain
CREATE sl:6 SET a = 'six', b = 6;
CREATE sl:7 SET a = 7, b = 'seven';
SELECT VALUE a FROM sl ORDER BY id;
SELECT VALUE b FROM sl ORDER BY id;
(INFO FOR TABLE sl).migrations.generation;
-- @check
-- A database at compatibility level 3: no table is enforced.
DEFINE DATABASE c3 COMPATIBILITY 3;
USE DB c3;
DEFINE TABLE p SCHEMAFULL;
DEFINE FIELD a ON p TYPE any;
CREATE p:1 SET a = '7';
CREATE p:2 SET a = 7;
CREATE p:3 SET a = NULL;
CREATE p:4 SET a = 'x';
ALTER FIELD a ON p TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
SELECT VALUE a FROM p ORDER BY id;
(INFO FOR TABLE p).migrations.generation;
-- @drain
ALTER FIELD a ON p TYPE float MIGRATE EXISTING ROWS AS a_f USING <float>$value;
SELECT VALUE a FROM p ORDER BY id;
(INFO FOR TABLE p).migrations.generation;
DEFINE FIELD OVERWRITE a ON p TYPE string;
CREATE p:5 SET a = 'five';
SELECT VALUE a FROM p ORDER BY id;
-- @check
-- @drain
SELECT VALUE a FROM p ORDER BY id;
(INFO FOR TABLE p).migrations.generation;
-- @check
