-- A migration that makes two rows' values collide, and the indexes over its field. An index defined in
-- the transaction that runs the migration is refused before it writes, so that transaction commits the
-- migration and the writes beside it, a value a UNIQUE index would refuse among them. After the job has
-- drained, a UNIQUE index over the migrated values is refused for the duplicates after its build wrote,
-- which refuses the commit of its transaction, so the write beside it is not kept either. A non-unique
-- index over the field is then removed and redefined with a failing write after them, which commits,
-- and the index answers as a scan of the migrated rows does.
DEFINE TABLE uq2 SCHEMAFULL;
DEFINE FIELD k ON uq2 TYPE string;
CREATE uq2:1 SET k = '1' RETURN NONE;
CREATE uq2:2 SET k = '01' RETURN NONE;
CREATE uq2:3 SET k = '2' RETURN NONE;
-- @upgrade
-- @client
ALTER FIELD k ON uq2 TYPE int MIGRATE EXISTING ROWS AS k_int USING <int>$value FALLBACK 0;
DEFINE INDEX uk ON uq2 FIELDS k UNIQUE;
CREATE uq2:4 SET k = 4 RETURN NONE;
CREATE uq2:5 SET k = 1 RETURN NONE;
DEFINE INDEX uk2 ON uq2 FIELDS k;
-- @end
SELECT * FROM uq2 ORDER BY id;
SELECT VALUE id FROM uq2 WHERE k = 1 ORDER BY id;
(INFO FOR TABLE uq2).indexes;
(INFO FOR TABLE uq2).migrations;
-- @check
-- @drain
SELECT * FROM uq2 ORDER BY id;
SELECT VALUE id FROM uq2 WHERE k = 1 ORDER BY id;
(INFO FOR TABLE uq2).migrations;
-- @check
-- After the job, the UNIQUE index is refused for the duplicates the migration made, and its
-- transaction keeps nothing.
-- @client
DEFINE INDEX uk ON uq2 FIELDS k UNIQUE;
CREATE uq2:6 SET k = 6 RETURN NONE;
-- @end
SELECT VALUE id FROM uq2 ORDER BY id;
(INFO FOR TABLE uq2).indexes;
-- @check
-- The replacement is a non-unique index, whose removal and redefinition commit with a failing write
-- after them.
DEFINE INDEX uk2 ON uq2 FIELDS k;
-- @client
REMOVE INDEX uk2 ON uq2;
DEFINE INDEX uk3 ON uq2 FIELDS k;
CREATE uq2:6 SET k = 'six' RETURN NONE;
-- @end
SELECT VALUE id FROM uq2 WHERE k = 1 ORDER BY id;
(INFO FOR TABLE uq2).indexes;
-- @check
-- Control: the same collision, with the migration committed and drained before the UNIQUE index is
-- defined in a unit of its own. The definition must be refused for the duplicate migrated values.
DEFINE TABLE uq3 SCHEMAFULL;
DEFINE FIELD k ON uq3 TYPE string;
CREATE uq3:1 SET k = '1' RETURN NONE;
CREATE uq3:2 SET k = '01' RETURN NONE;
ALTER FIELD k ON uq3 TYPE int MIGRATE EXISTING ROWS AS k_int USING <int>$value FALLBACK 0;
-- @drain
DEFINE INDEX u3 ON uq3 FIELDS k UNIQUE;
(INFO FOR TABLE uq3).indexes;
SELECT VALUE id FROM uq3 WHERE k = 1 ORDER BY id;
-- @check
