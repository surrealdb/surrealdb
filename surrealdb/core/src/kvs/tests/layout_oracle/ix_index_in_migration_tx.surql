-- A migration and an index over the migrated field committed together: the index is built from the
-- migrated values whether DEFINE INDEX follows ALTER inside a BEGIN block or inside a transaction its
-- caller commits. A statement that fails in the caller's transaction keeps what it wrote, and the
-- index must still answer as a scan of the migrated rows does.
DEFINE TABLE tx SCHEMAFULL;
DEFINE FIELD a ON tx TYPE string;
DEFINE FIELD c ON tx TYPE string;
CREATE tx:1 SET a = '5', c = '50' RETURN NONE;
CREATE tx:2 SET a = '6', c = '60' RETURN NONE;
CREATE tx:3 SET a = 'junk', c = '7' RETURN NONE;
-- @upgrade
BEGIN;
ALTER FIELD a ON tx TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK 0;
DEFINE INDEX ia ON tx FIELDS a;
COMMIT;
SELECT VALUE id FROM tx WHERE a = 5 ORDER BY id;
SELECT VALUE id FROM tx WHERE a = 5 ORDER BY id EXPLAIN;
SELECT VALUE id FROM tx WHERE a = '5' ORDER BY id;
SELECT VALUE id FROM tx WHERE a >= 0 ORDER BY id;
-- @check
-- @client
ALTER FIELD c ON tx TYPE int MIGRATE EXISTING ROWS AS c_int USING <int>$value FALLBACK 0;
DEFINE INDEX ic ON tx FIELDS c;
CREATE tx:4 SET a = 4, c = 'bad' RETURN NONE;
CREATE tx:5 SET a = 'bad', c = 80 RETURN NONE;
SELECT VALUE id FROM tx WHERE c = 50 ORDER BY id;
-- @end
SELECT VALUE id FROM tx WHERE c = 50 ORDER BY id;
SELECT VALUE id FROM tx WHERE c >= 7 ORDER BY id;
SELECT VALUE id FROM tx WHERE c >= 7 ORDER BY id EXPLAIN;
SELECT VALUE c FROM tx ORDER BY id;
-- @drain
SELECT VALUE c FROM tx ORDER BY id;
SELECT VALUE id FROM tx WHERE c = 0 ORDER BY id;
-- @check
-- Control: the same kind of migration committed alone, with the index defined in a unit of its own.
DEFINE FIELD e ON tx TYPE option<string>;
UPDATE tx:1 SET e = '3' RETURN NONE;
UPDATE tx:2 SET e = '4' RETURN NONE;
UPDATE tx:3 SET e = 'z' RETURN NONE;
BEGIN;
ALTER FIELD e ON tx TYPE option<int> MIGRATE EXISTING ROWS AS e_int USING <int>$value FALLBACK NONE;
COMMIT;
DEFINE INDEX ie ON tx FIELDS e;
SELECT VALUE id FROM tx WHERE e = 3 ORDER BY id;
SELECT VALUE id FROM tx WHERE e = 3 ORDER BY id EXPLAIN;
SELECT VALUE id FROM tx WHERE e > 3 ORDER BY id;
-- @drain
SELECT VALUE e FROM tx ORDER BY id;
SELECT VALUE id FROM tx WHERE e = 3 ORDER BY id;
-- @check
-- Whether a REBUILD of the index built inside the transaction repairs it.
REBUILD INDEX ia ON tx;
SELECT VALUE id FROM tx WHERE a = 5 ORDER BY id;
SELECT VALUE id FROM tx WHERE a = 5 ORDER BY id EXPLAIN;
SELECT VALUE id FROM tx WHERE a > 0 ORDER BY id;
-- @check
