-- A string field narrowed to int by a fallible cast. '12' and '-3' convert;
-- 'x', '', '1.5', ' 7' and an out-of-range integer string take the FALLBACK.
-- NULL and an absent field reach the step as legacy values (TYPE any stores
-- them). A string-to-int step without a FALLBACK is refused. Probes what the
-- step writes into the NULL and absent rows, and that an index over the new
-- type agrees with a scan. An index that reads the field blocks a migration, so
-- it is removed before each later migration and defined again after.
DEFINE TABLE s1 SCHEMAFULL;
DEFINE FIELD f ON s1 TYPE any;
DEFINE FIELD name ON s1 TYPE string;
CREATE s1:1 SET f = '12', name = 'a';
CREATE s1:2 SET f = 'x', name = 'b';
CREATE s1:3 SET f = '1.5', name = 'c';
CREATE s1:4 SET f = ' 7', name = 'd';
CREATE s1:5 SET f = '', name = 'e';
CREATE s1:6 SET f = '9223372036854775808', name = 'f';
CREATE s1:7 SET f = NULL, name = 'g';
CREATE s1:8 SET name = 'h';
CREATE s1:9 SET f = '-3', name = 'i';
CREATE s1:10 SET f = 4, name = 'j';
SELECT VALUE f FROM s1 ORDER BY id;
-- @upgrade
ALTER FIELD f ON s1 TYPE int MIGRATE EXISTING ROWS AS f_int USING <int>$value;
ALTER FIELD f ON s1 TYPE int MIGRATE EXISTING ROWS AS f_int USING <int>$value FALLBACK -1;
SELECT VALUE f FROM s1 ORDER BY id;
SELECT VALUE f FROM ONLY s1:7;
SELECT VALUE f FROM ONLY s1:8;
(INFO FOR TABLE s1).migrations;
-- @check
DEFINE INDEX by_f ON s1 FIELDS f;
SELECT VALUE name FROM s1 WHERE f = -1 ORDER BY id;
SELECT VALUE name FROM s1 WHERE f >= 0 ORDER BY id;
SELECT VALUE name FROM s1 WHERE f < 0 ORDER BY id;
-- @drain
SELECT VALUE f FROM s1 ORDER BY id;
(INFO FOR TABLE s1).migrations;
-- Rows written after the change are held to int.
CREATE s1:11 SET f = 11, name = 'k';
CREATE s1:12 SET f = 'y', name = 'l';
CREATE s1:13 SET f = '14', name = 'm';
SELECT VALUE f FROM s1 ORDER BY id;
-- The change back to string, FALLBACK a string. The index is removed first.
REMOVE INDEX by_f ON s1;
ALTER FIELD f ON s1 TYPE string MIGRATE EXISTING ROWS AS f_str USING <string>$value;
ALTER FIELD f ON s1 TYPE string MIGRATE EXISTING ROWS AS f_str USING <string>$value FALLBACK 'none';
SELECT VALUE f FROM s1 ORDER BY id;
DEFINE INDEX by_f ON s1 FIELDS f;
SELECT VALUE name FROM s1 WHERE f = 'none' ORDER BY id;
SELECT VALUE name FROM s1 WHERE f = '12' ORDER BY id;
-- @check
-- @drain
SELECT VALUE f FROM s1 ORDER BY id;
