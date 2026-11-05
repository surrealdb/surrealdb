-- COMPUTED fields over a nested field and over an array, both changed by a pending migration. Each computed
-- field is defined once its input's migration is minted, so every row it reads is read through the step; a
-- computed field chained on another reads through it; a computed field with a TYPE answers what its
-- expression yields; an index over a computed field answers as a scan does; and a write to a nested input or
-- to the array moves every value that reads it.
DEFINE TABLE nm SCHEMAFULL;
DEFINE FIELD o ON nm TYPE object;
DEFINE FIELD o.n ON nm TYPE string;
DEFINE FIELD tags ON nm TYPE array<string>;
DEFINE FIELD k ON nm TYPE option<int>;
CREATE nm:1 SET o = { n: '4' }, tags = ['1', '2'] RETURN NONE;
CREATE nm:2 SET o = { n: '10' }, tags = ['5'] RETURN NONE;
CREATE nm:3 SET o = { n: 'x' }, tags = [] RETURN NONE;
CREATE nm:4 SET o = { n: '7' }, tags = ['3', 'y'] RETURN NONE;
-- @upgrade
ALTER FIELD o.n ON nm TYPE int MIGRATE EXISTING ROWS AS o_int USING <int>$value FALLBACK 0;
ALTER FIELD tags ON nm TYPE array<int> MIGRATE EXISTING ROWS AS tags_int USING <array<int>>$value FALLBACK [];
DEFINE FIELD dbl ON nm TYPE int COMPUTED (o.n ?? 0) * 2;
DEFINE FIELD total ON nm TYPE option<int> COMPUTED math::sum(tags);
DEFINE FIELD label ON nm TYPE string COMPUTED string::concat('n=', <string>(o.n ?? 0));
DEFINE FIELD big ON nm COMPUTED total > 5 OR dbl > 15;
SELECT * FROM nm ORDER BY id;
SELECT VALUE id FROM nm WHERE total > 2 ORDER BY id;
DEFINE INDEX by_total ON nm FIELDS total;
SELECT VALUE id FROM nm WHERE total > 2 ORDER BY id;
SELECT VALUE id FROM nm WHERE dbl = 8 ORDER BY id;
-- @check
-- @drain
SELECT * FROM nm ORDER BY id;
(INFO FOR TABLE nm).migrations.guaranteed;
-- A write to a nested input and to the array moves the values that read them.
UPDATE nm:1 SET o.n = 100 RETURN NONE;
UPDATE nm:3 SET tags = [9, 9] RETURN NONE;
UPDATE nm:4 SET tags += 4 RETURN NONE;
SELECT * FROM nm ORDER BY id;
SELECT VALUE id FROM nm WHERE total > 2 ORDER BY id;
-- Values the nested TYPE and the array TYPE refuse leave their row as it was.
UPDATE nm:2 SET o.n = 'ten' RETURN NONE;
UPDATE nm:2 SET tags = ['a'] RETURN NONE;
SELECT * FROM nm ORDER BY id;
-- @check
-- Redefining a computed expression changes what every row reads and stores nothing.
DEFINE FIELD OVERWRITE total ON nm TYPE option<int> COMPUTED math::sum(tags) * 10;
SELECT VALUE total FROM nm ORDER BY id;
SELECT VALUE id FROM nm WHERE total > 20 ORDER BY id;
-- The nested field widened to option<int>: a write may now hold NONE, and the computed field over it is
-- judged on what it yields.
DEFINE FIELD OVERWRITE o.n ON nm TYPE option<int>;
UPDATE nm:2 SET o.n = NONE RETURN NONE;
SELECT * FROM nm ORDER BY id;
-- @check
-- @drain
SELECT * FROM nm ORDER BY id;
(INFO FOR TABLE nm).migrations.generation;
(INFO FOR TABLE nm).migrations.guaranteed;
-- @check
