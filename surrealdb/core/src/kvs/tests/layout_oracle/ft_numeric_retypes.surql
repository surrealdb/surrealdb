-- Numeric TYPE changes on a populated table. A string field becomes number
-- through a fallible cast. Once the job has drained and the claims state the
-- source type is guaranteed, a cast of it to float is proven total, so it takes
-- no FALLBACK. Probed: whether that cast keeps integers past 2^53 and decimals
-- with more digits than a float holds (rounded, or not), and whether the FALLBACK
-- is taken when an explicit <float> cast rounds instead of failing. Float to int
-- is a narrowing: a fractional or out-of-range float must not be silently truncated.
-- The claim is checked after a drain, and the last section probes the same cast
-- before any drain, where the claim is not yet recorded.
DEFINE TABLE n1 SCHEMAFULL;
DEFINE FIELD v ON n1 TYPE int;
DEFINE FIELD w ON n1 TYPE string;
DEFINE FIELD dec ON n1 TYPE string;
DEFINE FIELD z ON n1 TYPE string;
DEFINE FIELD q ON n1 TYPE int;
CREATE n1:1 SET v = 1, w = '1', dec = '1.5', z = '9007199254740993', q = 1;
CREATE n1:2 SET v = 9007199254740993, w = '2.5', dec = '2', z = '7', q = 2;
CREATE n1:3 SET v = -7, w = '3dec', dec = 'x', z = 'x', q = 3;
CREATE n1:4 SET v = 0, w = '0.1', dec = '', z = '', q = 4;
CREATE n1:5 SET v = 42, w = 'x', dec = '-0.25', z = '1e3', q = 5;
CREATE n1:6 SET v = 5, w = '1e3', dec = '7.000001', z = '2.5', q = 6;
SELECT VALUE w FROM n1 ORDER BY id;
-- @upgrade
ALTER FIELD w ON n1 TYPE number MIGRATE EXISTING ROWS AS w_num USING <number>$value FALLBACK 0;
ALTER FIELD dec ON n1 TYPE decimal MIGRATE EXISTING ROWS AS dec_dec USING <decimal>$value FALLBACK 0dec;
ALTER FIELD z ON n1 TYPE int MIGRATE EXISTING ROWS AS z_int USING <int>$value FALLBACK 0;
ALTER FIELD q ON n1 TYPE decimal MIGRATE EXISTING ROWS AS q_dec USING <decimal>$value;
ALTER FIELD q ON n1 TYPE decimal MIGRATE EXISTING ROWS AS q_dec USING <decimal>$value FALLBACK 0dec;
SELECT VALUE w FROM n1 ORDER BY id;
SELECT VALUE dec FROM n1 ORDER BY id;
SELECT VALUE z FROM n1 ORDER BY id;
-- @drain
(INFO FOR TABLE n1).migrations.guaranteed;
-- Number and decimal are guaranteed by now: casts of them to float need no
-- FALLBACK. The decimal row holds more digits than a float can.
CREATE n1:7 SET v = 7, w = 7, dec = 1.00000000000000000000000000001dec, z = 7, q = 7dec;
ALTER FIELD w ON n1 TYPE float MIGRATE EXISTING ROWS AS w_float USING <float>$value;
ALTER FIELD dec ON n1 TYPE float MIGRATE EXISTING ROWS AS dec_float USING <float>$value;
SELECT VALUE w FROM n1 ORDER BY id;
SELECT VALUE dec FROM n1 ORDER BY id;
-- @drain
-- Int is guaranteed for z: a cast of it to float needs no FALLBACK, though
-- 9007199254740993 has no exact float.
(INFO FOR TABLE n1).migrations.guaranteed;
ALTER FIELD z ON n1 TYPE float MIGRATE EXISTING ROWS AS z_float USING <float>$value;
SELECT VALUE z FROM n1 ORDER BY id;
-- v was never migrated: the same cast is not proven total and needs a FALLBACK.
ALTER FIELD v ON n1 TYPE float MIGRATE EXISTING ROWS AS v_float USING <float>$value;
ALTER FIELD v ON n1 TYPE float MIGRATE EXISTING ROWS AS v_float USING <float>$value FALLBACK 0f;
SELECT VALUE v FROM n1 ORDER BY id;
-- @drain
-- Float to int: a fractional or out-of-range float is a lossy coercion.
ALTER FIELD w ON n1 TYPE int MIGRATE EXISTING ROWS AS w_int USING <int>$value;
ALTER FIELD w ON n1 TYPE int MIGRATE EXISTING ROWS AS w_int USING <int>$value FALLBACK -1;
SELECT VALUE w FROM n1 ORDER BY id;
-- @check
DEFINE INDEX by_w ON n1 FIELDS w;
SELECT VALUE id FROM n1 WHERE w = -1 ORDER BY id;
SELECT VALUE id FROM n1 WHERE w >= 1 ORDER BY id;
-- @drain
-- Rows written after the changes are held to the new types.
CREATE n1:8 SET v = 8f, w = 8, dec = 8f, z = 8f, q = 8dec;
CREATE n1:9 SET v = 9.5f, w = 9, dec = 9f, z = 9f, q = 9dec;
CREATE n1:10 SET v = 10f, w = 8.5, dec = 10f, z = 10f, q = 10dec;
SELECT VALUE w FROM n1 ORDER BY id;
-- Widening a float to number needs no migration and mints nothing.
DEFINE FIELD OVERWRITE v ON n1 TYPE number;
CREATE n1:11 SET v = 2.5, w = 11, dec = 11f, z = 11f, q = 11dec;
SELECT VALUE v FROM n1 ORDER BY id;
-- @check
-- Two fields migrated to non-float types, with the claims read after each step.
-- A claim on one field is forfeited by a change to any field of the table, so the
-- claim on w, settled by the eager job after its own migration, does not survive
-- the migration of d. A cast of w with no FALLBACK is accepted once w's claim
-- holds, so the answer depends on whether the job drained between the two steps.
DEFINE TABLE gl SCHEMAFULL;
DEFINE FIELD w ON gl TYPE string;
DEFINE FIELD d ON gl TYPE string;
CREATE gl:1 SET w = '1', d = '2';
CREATE gl:2 SET w = 'x', d = 'y';
-- @upgrade
ALTER FIELD w ON gl TYPE number MIGRATE EXISTING ROWS AS gl_w USING <number>$value FALLBACK 0;
(INFO FOR TABLE gl).migrations.guaranteed;
ALTER FIELD d ON gl TYPE decimal MIGRATE EXISTING ROWS AS gl_d USING <decimal>$value FALLBACK 0dec;
(INFO FOR TABLE gl).migrations.guaranteed;
-- @drain
(INFO FOR TABLE gl).migrations.guaranteed;
ALTER FIELD w ON gl TYPE float MIGRATE EXISTING ROWS AS gl_wf USING <float>$value;
SELECT VALUE w FROM gl ORDER BY id;
-- @check
-- Integers past 2^53 through a float, one field per table so that no other
-- change forfeits its claim. An explicit <float> cast rounds the integer, and the
-- FALLBACK is not taken; the implicit $value form refuses an inexact float.
DEFINE TABLE rd SCHEMAFULL;
DEFINE FIELD z ON rd TYPE string;
CREATE rd:1 SET z = '9007199254740993';
CREATE rd:2 SET z = '7';
DEFINE TABLE rd2 SCHEMAFULL;
DEFINE FIELD z ON rd2 TYPE string;
CREATE rd2:1 SET z = '9007199254740993';
CREATE rd2:2 SET z = '7';
-- @upgrade
ALTER FIELD z ON rd TYPE int MIGRATE EXISTING ROWS AS z_int USING <int>$value FALLBACK 0;
ALTER FIELD z ON rd2 TYPE int MIGRATE EXISTING ROWS AS z_int USING <int>$value FALLBACK 0;
-- @drain
(INFO FOR TABLE rd).migrations.guaranteed;
ALTER FIELD z ON rd TYPE float MIGRATE EXISTING ROWS AS z_float USING <float>$value;
SELECT VALUE z FROM rd ORDER BY id;
ALTER FIELD z ON rd2 TYPE float MIGRATE EXISTING ROWS AS z_float USING $value FALLBACK -1f;
SELECT VALUE z FROM rd2 ORDER BY id;
-- @drain
-- Whether a decimal literal of 30 significant digits keeps its digits.
RETURN 1.00000000000000000000000000001dec = 1dec;
