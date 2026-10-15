-- Removing a field that an index and an event read, and a field a COMPUTED field reads on a second
-- table. An index over the field refuses the removal of a populated enforced table until the index is
-- removed; afterwards the event sees NONE for the field and the rows hold no stored value for it. A
-- COMPUTED field over a removed field is evaluated over NONE on every row, and a name removed that way
-- and re-declared as COMPUTED shows the computed value and no stored value of its earlier life, by
-- index, by scan and by filter.
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD x ON item TYPE int;
DEFINE FIELD y ON item TYPE option<int>;
DEFINE INDEX by_x ON item FIELDS x;
DEFINE EVENT ev ON item WHEN $event != "DELETE" THEN (UPSERT audit:last SET bef = $before.x, aft = $after.x, seen = $value.id);
CREATE item:1 SET x = 1, y = 1 RETURN NONE;
CREATE item:2 SET x = 2 RETURN NONE;
DEFINE TABLE cc SCHEMAFULL;
DEFINE FIELD x ON cc TYPE int;
DEFINE FIELD c ON cc COMPUTED (x ?? 0) * 2;
CREATE cc:1 SET x = 1 RETURN NONE;
CREATE cc:2 SET x = 2 RETURN NONE;
-- @upgrade
UPDATE item:1 SET y = 5 RETURN NONE;
SELECT * FROM audit ORDER BY id;
SELECT * FROM item ORDER BY id;
SELECT VALUE id FROM item WHERE x = 2;
-- @check
-- Refused while the index reads x; the removal goes through once the index is gone.
REMOVE FIELD x ON item;
REMOVE INDEX by_x ON item;
REMOVE FIELD x ON item;
SELECT * FROM item ORDER BY id;
SELECT VALUE id FROM item WHERE x = 2;
(INFO FOR TABLE item).fields;
-- The event sees NONE for the removed field.
UPDATE item:2 SET y = 7 RETURN NONE;
SELECT * FROM audit ORDER BY id;
SELECT * FROM item ORDER BY id;
-- @check
-- @drain
SELECT * FROM item ORDER BY id;
-- Re-declared as a computed field of the same name, no stored value may show.
DEFINE FIELD OVERWRITE x ON item COMPUTED 5;
SELECT * FROM item ORDER BY id;
SELECT VALUE id FROM item WHERE x = 5 ORDER BY id;
-- A stored y removed and re-declared as a computed field that reads x.
REMOVE FIELD y ON item;
DEFINE FIELD y ON item COMPUTED x + 1;
SELECT * FROM item ORDER BY id;
UPDATE item:1 SET y = 9 RETURN NONE;
SELECT * FROM item ORDER BY id;
-- @check
-- @drain
SELECT * FROM item ORDER BY id;
(INFO FOR TABLE item).migrations;
-- A COMPUTED field over a stored field on another table: the removal is not refused for it, and the
-- computed expression sees NONE on every row.
REMOVE FIELD x ON cc;
SELECT VALUE id FROM cc ORDER BY id;
SELECT * FROM cc ORDER BY id;
DEFINE FIELD OVERWRITE x ON cc COMPUTED 5;
SELECT * FROM cc ORDER BY id;
-- @check
