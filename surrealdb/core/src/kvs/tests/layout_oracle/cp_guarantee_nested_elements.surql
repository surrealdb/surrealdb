-- Guarantees on nested paths: a field under an object, the elements of an array, and a field under the elements
-- of an array of objects, with a top-level field of the same kind of migration as a control. Each is migrated;
-- after the job the claims hold for every row; then writes that would break one are tried in every form SurrealQL
-- allows (SET on a nested path, += append, a whole-array SET, an element SET by index). A refused write leaves its
-- row as it was; an accepted one keeps every claim true.
DEFINE TABLE gn SCHEMAFULL;
DEFINE FIELD k ON gn TYPE string;
DEFINE FIELD o ON gn TYPE object;
DEFINE FIELD o.n ON gn TYPE string;
DEFINE FIELD tags ON gn TYPE array<string>;
DEFINE FIELD items ON gn TYPE array<object>;
DEFINE FIELD items.*.qty ON gn TYPE string;
CREATE gn:1 SET k = '1', o = { n: '1' }, tags = ['1', '2'], items = [{ qty: '3' }, { qty: '4' }] RETURN NONE;
CREATE gn:2 SET k = 'x', o = { n: 'x' }, tags = [], items = [{ qty: '5' }] RETURN NONE;
CREATE gn:3 SET k = '3', o = { n: '3' }, tags = ['7'], items = [] RETURN NONE;
-- @upgrade
ALTER FIELD k ON gn TYPE int MIGRATE EXISTING ROWS AS k_int USING <int>$value FALLBACK 0;
ALTER FIELD o.n ON gn TYPE int MIGRATE EXISTING ROWS AS o_int USING <int>$value FALLBACK 0;
ALTER FIELD tags ON gn TYPE array<int> MIGRATE EXISTING ROWS AS tags_int USING <array<int>>$value FALLBACK [];
ALTER FIELD items.*.qty ON gn TYPE int MIGRATE EXISTING ROWS AS qty_int USING <int>$value FALLBACK 0;
SELECT * FROM gn ORDER BY id;
(INFO FOR TABLE gn).migrations.guaranteed;
-- @check
-- @drain
(INFO FOR TABLE gn).migrations.guaranteed;
(INFO FOR TABLE gn).migrations.verification;
SELECT * FROM gn ORDER BY id;
-- Writes that would break a claim, in each form.
UPDATE gn:1 SET o.n = 'bad' RETURN NONE;
UPDATE gn:1 SET tags += 'bad' RETURN NONE;
UPDATE gn:1 SET items[0].qty = 'bad' RETURN NONE;
UPDATE gn:1 SET items += { qty: 'bad' } RETURN NONE;
UPDATE gn:2 SET items = [{ qty: 1 }, { qty: 'z' }] RETURN NONE;
UPDATE gn:2 SET tags = [1, 'two'] RETURN NONE;
UPDATE gn:3 SET items += { qty: 9 } RETURN NONE;
UPDATE gn:3 SET items[0].qty = 10 RETURN NONE;
SELECT * FROM gn ORDER BY id;
(INFO FOR TABLE gn).migrations.guaranteed;
-- @check
-- Writes to the nested fields that keep their types hold.
UPDATE gn:1 SET o.n = 10 RETURN NONE;
UPDATE gn:2 SET tags += 9 RETURN NONE;
UPDATE gn:2 SET items[0].qty = 12 RETURN NONE;
SELECT * FROM gn ORDER BY id;
-- The nested field widened: a write may now hold NONE for it, and the old values stay.
DEFINE FIELD OVERWRITE o.n ON gn TYPE option<int>;
UPDATE gn:2 SET o.n = NONE RETURN NONE;
(INFO FOR TABLE gn).migrations.guaranteed;
SELECT * FROM gn ORDER BY id;
-- @check
-- The element field removed while rows hold values under it, then declared again: the stored elements keep
-- their values, and the new declaration is judged against them.
REMOVE FIELD items.*.qty ON gn;
SELECT * FROM gn ORDER BY id;
(INFO FOR TABLE gn).migrations.guaranteed;
DEFINE FIELD items.*.qty ON gn TYPE int;
UPDATE gn:1 SET items += { qty: 'after' } RETURN NONE;
SELECT * FROM gn ORDER BY id;
-- @check
-- @drain
(INFO FOR TABLE gn).migrations;
SELECT * FROM gn ORDER BY id;
-- @check
