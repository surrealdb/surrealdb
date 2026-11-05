-- Set elements migrated onto a shared value. The element step rewrites each element once; two
-- elements that now hold the same string are one element of the set; an empty or absent set is left
-- alone; a later write sees the collapsed set; an element kind the set does not admit is refused.
DEFINE TABLE st SCHEMAFULL;
DEFINE FIELD labels ON st TYPE option<set<string>>;
DEFINE FIELD labels.* ON st TYPE string;
CREATE st:1 SET labels = <set>['1', '01', 'z'] RETURN NONE;
CREATE st:2 SET labels = <set>[] RETURN NONE;
CREATE st:3 RETURN NONE;
CREATE st:5 SET labels = <set>['2', '20'] RETURN NONE;
-- @upgrade
CREATE st:6 SET labels = <set>['3', '03'] RETURN NONE;
ALTER FIELD labels.* ON st MIGRATE EXISTING ROWS AS strip_zero USING string::replace($value, '0', '') FALLBACK '';
(INFO FOR TABLE st).migrations;
SELECT * FROM st ORDER BY id;
SELECT id, array::len(<array> labels ?? []) AS n FROM st ORDER BY id;
-- @check
-- Refused: the set declares strings, so an int element does not fit.
ALTER FIELD labels.* ON st TYPE int MIGRATE EXISTING ROWS AS el_int USING <int>$value FALLBACK 0;
UPDATE st:1 SET labels += '1' RETURN AFTER;
UPDATE st:5 SET labels += '2' RETURN AFTER;
CREATE st:7 SET labels = <set>['4', '40', '4'] RETURN AFTER;
DEFINE INDEX by_l ON st FIELDS labels.*;
SELECT VALUE id FROM st WHERE labels CONTAINS '1' ORDER BY id;
SELECT VALUE id FROM st WHERE labels CONTAINS '1' EXPLAIN;
-- @drain
SELECT * FROM st ORDER BY id;
SELECT id, array::len(<array> labels ?? []) AS n FROM st ORDER BY id;
-- @check
