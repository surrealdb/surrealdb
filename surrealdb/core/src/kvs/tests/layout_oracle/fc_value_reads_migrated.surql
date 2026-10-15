-- A VALUE clause that reads other fields of the row, one of them a field a
-- migration changed: a write to the row runs the VALUE over the migrated input,
-- an old row not written keeps the value it stored, an index over the field
-- answers the same as a scan, and dropping the VALUE stops it running without
-- touching what it stored.
DEFINE TABLE s SCHEMAFULL;
DEFINE FIELD a ON s TYPE string;
DEFINE FIELD b ON s TYPE option<int>;
DEFINE FIELD total ON s TYPE option<int>;
CREATE s:1 SET a = '1', b = 1 RETURN NONE;
CREATE s:2 SET a = '20', b = 2, total = 22 RETURN NONE;
CREATE s:3 SET a = 'x' RETURN NONE;
-- @upgrade
ALTER FIELD a ON s TYPE int MIGRATE EXISTING ROWS AS a_int USING <int>$value FALLBACK -1;
ALTER FIELD total ON s VALUE (a ?? 0) + (b ?? 0);
SELECT * FROM s ORDER BY id;
-- The VALUE recomputes total from the migrated a on a write of the row.
UPDATE s:1 SET b = 5 RETURN NONE;
-- The fallback row: -1 plus b.
UPDATE s:3 SET b = 7 RETURN NONE;
-- Writing a row that holds total, without touching a or b.
UPDATE s:2 SET total = 99 RETURN NONE;
SELECT * FROM s ORDER BY id;
DEFINE INDEX by_total ON s FIELDS total;
SELECT VALUE id FROM s WHERE total > 0 ORDER BY id;
SELECT VALUE id FROM s WHERE total < 10 ORDER BY id;
-- @check
-- @drain
SELECT * FROM s ORDER BY id;
UPDATE s:2 SET a = 21 RETURN NONE;
SELECT * FROM s ORDER BY id;
-- A VALUE that reads a field no row holds, and one that reads itself.
DEFINE FIELD ghost ON s TYPE option<int> VALUE (missing ?? 0) + 1;
DEFINE FIELD self ON s TYPE option<int> VALUE ($value ?? 0) + 1;
UPDATE s:1 SET b = 6 RETURN NONE;
SELECT id, ghost, self FROM s ORDER BY id;
-- Dropping the VALUE: the stored total stays, the next write leaves it alone.
ALTER FIELD total ON s DROP VALUE;
UPDATE s:1 SET b = 8 RETURN NONE;
UPDATE s:2 SET b = 3 RETURN NONE;
SELECT * FROM s ORDER BY id;
SELECT VALUE id FROM s WHERE total > 0 ORDER BY id;
-- @check
(INFO FOR TABLE s).fields;
(INFO FOR TABLE s).migrations.generation;
(INFO FOR TABLE s).migrations.guaranteed;
