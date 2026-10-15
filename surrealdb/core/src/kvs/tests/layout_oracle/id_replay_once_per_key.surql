-- A row behind a step that is updated, upserted, deleted and created again under the same record id, or
-- written inside the caller's own transaction, applies each step to a stored value exactly once. A row
-- created under a key a step once covered is stored at the latest generation and is not replayed.
DEFINE TABLE m SCHEMAFULL;
DEFINE FIELD n ON m TYPE string;
DEFINE FIELD note ON m TYPE option<string>;
CREATE m:1 SET n = '5' RETURN NONE;
CREATE m:2 SET n = '6' RETURN NONE;
CREATE m:3 SET n = '7' RETURN NONE;
CREATE m:4 SET n = '8' RETURN NONE;
-- @upgrade
ALTER FIELD n ON m TYPE int MIGRATE EXISTING ROWS AS n_x10 USING (<int>$value) * 10 FALLBACK -1;
UPDATE m:1 SET note = 'a' RETURN AFTER;
SELECT VALUE n FROM ONLY m:1;
UPSERT m:2 SET note = 'b' RETURN AFTER;
UPSERT m:2 SET note = 'c' RETURN AFTER;
SELECT VALUE n FROM ONLY m:2;
UPDATE m:3 SET n = n + 1 RETURN AFTER;
(m:3).n;
-- @check
UPDATE m:3 SET n = 8 RETURN AFTER;
SELECT VALUE n FROM m ORDER BY id;
DELETE m:1 RETURN BEFORE;
CREATE m:1 SET n = 9 RETURN AFTER;
SELECT VALUE n FROM m ORDER BY id;
-- @drain
SELECT VALUE n FROM m ORDER BY id;
-- @client
UPDATE m:4 SET note = 'd' RETURN AFTER;
DELETE m:4;
CREATE m:4 SET n = 3 RETURN AFTER;
SELECT VALUE n FROM ONLY m:4;
UPDATE m:2 SET note = 'e' RETURN AFTER;
-- @end
SELECT * FROM m ORDER BY id;
-- @drain
SELECT VALUE n FROM m ORDER BY id;
-- @check
