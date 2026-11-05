-- A float field narrowed to option<int> and then to int, through DEFINE FIELD
-- OVERWRITE. Integral floats convert exactly; fractional, out-of-range and
-- sign-of-zero floats are probed for truncation or a FALLBACK. An absent field
-- (NONE) is what option exists for: a change to option<int> keeps it absent
-- unless the transform writes a value, and removing option needs a value for
-- every NONE row. Probes whether an absent row acquires the FALLBACK value.
DEFINE TABLE fl SCHEMAFULL;
DEFINE FIELD f ON fl TYPE option<float>;
DEFINE FIELD note ON fl TYPE string;
CREATE fl:1 SET f = 1f, note = 'one';
CREATE fl:2 SET f = 2.5f, note = 'frac';
CREATE fl:3 SET f = -0f, note = 'negzero';
CREATE fl:4 SET note = 'absent';
CREATE fl:5 SET f = 1e30f, note = 'big';
CREATE fl:6 SET f = 9007199254740992f, note = 'edge';
CREATE fl:7 SET f = -2.75f, note = 'negfrac';
CREATE fl:11 SET f = <float>'NaN', note = 'nan';
CREATE fl:12 SET f = <float>'inf', note = 'inf';
SELECT VALUE f FROM fl ORDER BY id;
-- A decimal narrowed to int: a fractional decimal is lossy, and a decimal just
-- below an integer keeps its digits.
DEFINE TABLE fd SCHEMAFULL;
DEFINE FIELD d ON fd TYPE decimal;
CREATE fd:1 SET d = 1.5dec;
CREATE fd:2 SET d = -2.5dec;
CREATE fd:3 SET d = 3dec;
CREATE fd:4 SET d = 2.999999999999999999dec;
SELECT VALUE d FROM fd ORDER BY id;
-- @upgrade
ALTER FIELD d ON fd TYPE int MIGRATE EXISTING ROWS AS d_int USING <int>$value;
ALTER FIELD d ON fd TYPE int MIGRATE EXISTING ROWS AS d_int USING <int>$value FALLBACK -1;
SELECT VALUE d FROM fd ORDER BY id;
DEFINE FIELD OVERWRITE f ON fl TYPE option<int> MIGRATE EXISTING ROWS AS f_opt_int USING <int>$value;
DEFINE FIELD OVERWRITE f ON fl TYPE option<int> MIGRATE EXISTING ROWS AS f_opt_int USING <int>$value FALLBACK -1;
SELECT VALUE f FROM fl ORDER BY id;
SELECT VALUE f FROM ONLY fl:4;
(INFO FOR TABLE fl).migrations.generation;
-- @check
DEFINE INDEX by_f ON fl FIELDS f;
SELECT VALUE note FROM fl WHERE f = -1 ORDER BY id;
SELECT VALUE note FROM fl WHERE f > 0 ORDER BY id;
SELECT VALUE note FROM fl WHERE f = NONE ORDER BY id;
-- @drain
SELECT VALUE f FROM fl ORDER BY id;
-- Removing option: the index is removed first, and every NONE row needs a value.
REMOVE INDEX by_f ON fl;
DEFINE FIELD OVERWRITE f ON fl TYPE int;
DEFINE FIELD OVERWRITE f ON fl TYPE int MIGRATE EXISTING ROWS AS f_int USING $value FALLBACK 0;
SELECT VALUE f FROM fl ORDER BY id;
SELECT VALUE f FROM ONLY fl:4;
(INFO FOR TABLE fl).migrations.generation;
(INFO FOR TABLE fl).migrations.guaranteed;
-- @check
-- Rows written after the change are held to int.
CREATE fl:8 SET f = 8, note = 'eight';
CREATE fl:9 SET f = 8.5, note = 'frac';
CREATE fl:10 SET note = 'none';
SELECT VALUE f FROM fl ORDER BY id;
-- A float field again, over a column of ints: not a widening, so refused without
-- MIGRATE; accepted with MIGRATE and a FALLBACK.
DEFINE FIELD OVERWRITE f ON fl TYPE float;
DEFINE FIELD OVERWRITE f ON fl TYPE float MIGRATE EXISTING ROWS AS f_float USING <float>$value FALLBACK 0f;
SELECT VALUE f FROM fl ORDER BY id;
(INFO FOR TABLE fl).migrations.generation;
-- @drain
SELECT VALUE f FROM fl ORDER BY id;
-- @check
