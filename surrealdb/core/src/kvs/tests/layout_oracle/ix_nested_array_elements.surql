-- Indexes over a nested path and over array elements, where the migrations rewrite both. A record
-- whose array holds '5' and '05' has two elements that are both 5 once migrated, so CONTAINS 5
-- through the index on t.* must return that record once. The index on o.n and the index on t.* are
-- each defined after their migration and before the job drains it.
DEFINE TABLE nx SCHEMAFULL;
DEFINE FIELD o ON nx TYPE option<object>;
DEFINE FIELD o.n ON nx TYPE string;
DEFINE FIELD t ON nx TYPE option<array>;
CREATE nx:1 SET o = { n: '5' }, t = ['5', '05'] RETURN NONE;
CREATE nx:2 SET o = { n: 'junk' }, t = ['7'] RETURN NONE;
CREATE nx:3 SET t = ['05'] RETURN NONE;
CREATE nx:4 SET o = { n: '7' } RETURN NONE;
CREATE nx:6 SET o = { n: '2' }, t = ['5', 'x'] RETURN NONE;
-- @upgrade
ALTER FIELD o.n ON nx TYPE int MIGRATE EXISTING ROWS AS on_int USING <int>$value FALLBACK 0;
ALTER FIELD t.* ON nx TYPE int MIGRATE EXISTING ROWS AS t_int USING <int>$value FALLBACK 0;
DEFINE INDEX ion ON nx FIELDS o.n;
DEFINE INDEX iel ON nx FIELDS t.*;
SELECT VALUE id FROM nx WHERE o.n = 5 ORDER BY id;
SELECT VALUE id FROM nx WHERE o.n = 5 ORDER BY id EXPLAIN;
SELECT VALUE id FROM nx WHERE t CONTAINS 5 ORDER BY id;
SELECT VALUE id FROM nx WHERE t CONTAINS 5 ORDER BY id EXPLAIN;
SELECT VALUE id FROM nx WHERE t CONTAINS '5' ORDER BY id;
SELECT VALUE id FROM nx WHERE t CONTAINSANY [5, 7] ORDER BY id;
SELECT VALUE id FROM nx WHERE o.n > 4 ORDER BY id;
SELECT VALUE id FROM nx WHERE o.n IN [0, 7] ORDER BY id;
-- @check
UPDATE nx:2 SET t = [5, 8] RETURN NONE;
UPDATE nx:3 SET o = { n: 5 } RETURN NONE;
CREATE nx:5 SET o = { n: 0 }, t = [0] RETURN NONE;
SELECT VALUE id FROM nx WHERE t CONTAINS 5 ORDER BY id;
SELECT VALUE id FROM nx WHERE o.n = 5 ORDER BY id;
DELETE nx:1;
SELECT VALUE id FROM nx WHERE t CONTAINS 5 ORDER BY id;
-- @drain
SELECT * FROM nx ORDER BY id;
SELECT VALUE id FROM nx WHERE t CONTAINS 5 ORDER BY id;
-- @check
