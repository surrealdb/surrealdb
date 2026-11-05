-- An ASSERT that reads a COMPUTED field, added over rows a migration has not yet reached. The assertion judges
-- the field's own new value against the computed value the row holds at that write; the verification counts
-- the rows that break it; a change to the computed input moves the value the assertion reads without writing
-- the asserted field, and the verification and the guarantee must still tell the truth for that row.
DEFINE TABLE ar SCHEMAFULL;
DEFINE FIELD raw ON ar TYPE string;
DEFINE FIELD lo ON ar TYPE option<int>;
CREATE ar:1 SET raw = '1', lo = 1 RETURN NONE;
CREATE ar:2 SET raw = '5', lo = 3 RETURN NONE;
CREATE ar:3 SET raw = 'x', lo = 0 RETURN NONE;
CREATE ar:4 SET raw = '4', lo = 9 RETURN NONE;
CREATE ar:5 SET raw = '6' RETURN NONE;
-- @upgrade
ALTER FIELD raw ON ar TYPE int MIGRATE EXISTING ROWS AS raw_int USING <int>$value FALLBACK 0;
DEFINE FIELD hi ON ar COMPUTED (raw ?? 0) * 2;
SELECT id, raw, lo, hi FROM ar ORDER BY id;
-- The assertion added over rows still behind the migration: the value it reads for each row is migrated.
ALTER FIELD lo ON ar ASSERT $value < hi;
(INFO FOR TABLE ar).migrations.verification;
SELECT id, raw, lo, hi FROM ar ORDER BY id;
SELECT VALUE id FROM ar WHERE hi > 5 ORDER BY id;
-- @check
-- @drain
(INFO FOR TABLE ar).migrations.verification;
(INFO FOR TABLE ar).migrations.guaranteed;
SELECT id, raw, lo, hi FROM ar ORDER BY id;
-- A write of the input moves hi below lo. The assertion on lo is judged on every write of its row, so this write is
-- refused while the new hi is below lo.
UPDATE ar:2 SET raw = 1 RETURN NONE;
SELECT id, raw, lo, hi FROM ar ORDER BY id;
(INFO FOR TABLE ar).migrations.verification;
(INFO FOR TABLE ar).migrations.guaranteed;
-- A write of lo is judged against the hi the row holds now.
UPDATE ar:2 SET lo = 3 RETURN NONE;
UPDATE ar:5 SET lo = 1 RETURN NONE;
SELECT id, raw, lo, hi FROM ar ORDER BY id;
-- @check
-- The computed field redefined: the assertion reads the new expression.
DEFINE FIELD OVERWRITE hi ON ar COMPUTED raw * 100;
(INFO FOR TABLE ar).migrations.verification;
UPDATE ar:3 SET lo = 0 RETURN NONE;
UPDATE ar:3 SET lo = 1 RETURN NONE;
SELECT * FROM ar ORDER BY id;
-- A second migration of the input, minted after the assertion: the rows behind it are judged on what it gives.
ALTER FIELD raw ON ar TYPE int MIGRATE EXISTING ROWS AS raw_plus USING $value + 1 FALLBACK 0;
SELECT id, raw, lo, hi FROM ar ORDER BY id;
(INFO FOR TABLE ar).migrations.verification;
-- @check
-- @drain
(INFO FOR TABLE ar).migrations.verification;
(INFO FOR TABLE ar).migrations.guaranteed;
SELECT id, raw, lo, hi FROM ar ORDER BY id;
-- @check
ALTER FIELD lo ON ar DROP ASSERT;
(INFO FOR TABLE ar).migrations.verification;
(INFO FOR TABLE ar).migrations.guaranteed;
-- @check
-- An unrelated field defined on the table: the guarantee on raw is read before and after the next drain.
DEFINE FIELD other ON ar TYPE option<int>;
(INFO FOR TABLE ar).migrations.guaranteed;
-- @drain
(INFO FOR TABLE ar).migrations.guaranteed;
-- @check
