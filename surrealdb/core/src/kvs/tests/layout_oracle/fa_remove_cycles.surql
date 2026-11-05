-- One name removed and re-declared over and over on a populated enforced table, each life of the
-- name with its own TYPE, DEFAULT and MIGRATE choice and writes in between. No value from an earlier
-- life returns after a removal, and a re-declared field starts empty unless its own MIGRATE fills it.
-- A removal and a re-declaration inside one transaction, and a refused statement inside one, follow
-- the same rules: whatever the transaction's statements wrote before a failure is kept.
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD k ON item TYPE option<int>;
CREATE item:1 SET k = 1 RETURN NONE;
CREATE item:2 SET k = 2 RETURN NONE;
-- @upgrade
REMOVE FIELD k ON item;
DEFINE FIELD k ON item TYPE option<string> DEFAULT 'd';
CREATE item:3 SET k = 'three' RETURN NONE;
SELECT * FROM item ORDER BY id;
REMOVE FIELD k ON item;
DEFINE FIELD k ON item TYPE option<int>;
CREATE item:4 SET k = 4 RETURN NONE;
SELECT * FROM item ORDER BY id;
-- @check
REMOVE FIELD k ON item;
SELECT * FROM item ORDER BY id;
DEFINE FIELD k ON item TYPE int MIGRATE EXISTING ROWS AS k_fill USING 0;
SELECT * FROM item ORDER BY id;
-- @drain
SELECT * FROM item ORDER BY id;
SELECT VALUE id FROM item WHERE k = 4 ORDER BY id;
(INFO FOR TABLE item).fields;
-- A removal and a re-declaration in one transaction, then a write under the new declaration.
-- @client
REMOVE FIELD k ON item;
DEFINE FIELD k ON item TYPE option<string>;
CREATE item:5 SET k = 'five' RETURN NONE;
-- @end
SELECT * FROM item ORDER BY id;
-- @check
-- A removal, then a refused re-declaration, then a write that the refusal makes undeclared. The
-- transaction is committed as it stands.
-- @client
REMOVE FIELD k ON item;
DEFINE FIELD k ON item TYPE int;
CREATE item:6 SET k = 6 RETURN NONE;
-- @end
SELECT * FROM item ORDER BY id;
(INFO FOR TABLE item).fields;
-- @check
-- Re-declared once more, with a MIGRATE, after the refused definition left the name free.
DEFINE FIELD k ON item TYPE option<int> MIGRATE EXISTING ROWS AS k_back USING $value ?? 42 FALLBACK 42;
SELECT * FROM item ORDER BY id;
-- @drain
SELECT * FROM item ORDER BY id;
(INFO FOR TABLE item).migrations;
-- @check
