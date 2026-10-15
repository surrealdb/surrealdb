-- Adding fields to a populated SCHEMAFULL table, each form in turn: an optional TYPE with
-- DEFAULT, DEFAULT ALWAYS, VALUE and ASSERT; a non-optional TYPE that is refused without MIGRATE,
-- with or without DEFAULT; and the same non-optional TYPE accepted with MIGRATE. Old rows read each
-- new field as its definition says, a refused definition leaves no field or name behind, and a
-- write made after the change is checked against every definition it meets.
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD name ON item TYPE string;
CREATE item:1 SET name = 'a' RETURN NONE;
CREATE item:2 SET name = 'b' RETURN NONE;
-- @upgrade
CREATE item:3 SET name = 'c' RETURN NONE;
DEFINE FIELD tag ON item TYPE option<string>;
DEFINE FIELD dflt ON item TYPE option<int> DEFAULT 7;
DEFINE FIELD alw ON item TYPE option<int> DEFAULT ALWAYS 9;
DEFINE FIELD val ON item TYPE option<int> VALUE $value ?? 3;
DEFINE FIELD asrt ON item TYPE option<int> ASSERT $value > 0;
-- Refused: rows exist and nothing says what they hold. The second is the same name, free again.
DEFINE FIELD plain ON item TYPE int;
DEFINE FIELD plain ON item TYPE int DEFAULT 1;
DEFINE FIELD plain ON item TYPE int ASSERT $value > 0;
(INFO FOR TABLE item).fields;
SELECT * FROM item ORDER BY id;
-- Accepted with MIGRATE: every old row gets the value.
DEFINE FIELD req ON item TYPE int MIGRATE EXISTING ROWS AS add_req USING 5;
DEFINE FIELD req2 ON item TYPE string DEFAULT 'd' MIGRATE EXISTING ROWS AS add_req2 USING $value ?? 'x' FALLBACK 'x';
SELECT * FROM item ORDER BY id;
-- Writes after the change: refused by ASSERT, refused by TYPE, and accepted.
CREATE item:4 SET name = 'd', req = 1, asrt = -1 RETURN NONE;
CREATE item:5 SET name = 'e', req = 1, tag = 5 RETURN NONE;
CREATE item:6 SET name = 'f', req = 1, asrt = 2 RETURN NONE;
UPDATE item:1 SET name = 'a2' RETURN NONE;
SELECT * FROM item ORDER BY id;
-- @check
-- @drain
SELECT * FROM item ORDER BY id;
SELECT VALUE id FROM item WHERE dflt = 7 ORDER BY id;
SELECT VALUE id FROM item WHERE req = 5 ORDER BY id;
SELECT VALUE id FROM item WHERE alw = 9 ORDER BY id;
(INFO FOR TABLE item).migrations;
-- OVERWRITE replaces the default for later writes only.
DEFINE FIELD OVERWRITE dflt ON item TYPE option<int> DEFAULT 8;
CREATE item:7 SET name = 'g', req = 1 RETURN NONE;
SELECT * FROM item ORDER BY id;
SELECT VALUE id FROM item WHERE dflt = 8 ORDER BY id;
-- A VALUE field added to a populated table is applied by writes only, not to the rows already there.
DEFINE FIELD late ON item TYPE option<int> VALUE $value ?? 1;
UPDATE item:2 SET name = 'b2' RETURN NONE;
SELECT * FROM item ORDER BY id;
-- @check
-- @drain
SELECT * FROM item ORDER BY id;
