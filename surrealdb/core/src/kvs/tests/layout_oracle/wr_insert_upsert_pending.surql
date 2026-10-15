-- INSERT (single, bulk, ON DUPLICATE KEY UPDATE) and UPSERT on rows behind a pending
-- step: the duplicate-key path reads the migrated value, a record created afterwards is
-- written at the current generation, a bulk INSERT with a failing row writes nothing,
-- and an index defined over the migrated field answers WHERE clauses with the migrated values.
DEFINE TABLE ins SCHEMAFULL;
DEFINE FIELD n ON ins TYPE string;
DEFINE FIELD hits ON ins TYPE option<int>;
CREATE ins:1 SET n = '1', hits = 0;
CREATE ins:2 SET n = '2', hits = 0;
CREATE ins:3 SET n = '3', hits = 0;
CREATE ins:6 SET n = '6', hits = 0;
-- @upgrade
CREATE ins:4 SET n = '4', hits = 0;
ALTER FIELD n ON ins TYPE int MIGRATE EXISTING ROWS AS n_plus USING <int>$value + 10 FALLBACK -1;
DEFINE INDEX by_n ON ins FIELDS n;
INSERT INTO ins { id: ins:1, n: 99, hits: 0 } ON DUPLICATE KEY UPDATE hits += 1 RETURN AFTER;
INSERT INTO ins [{ id: ins:2, n: 22, hits: 0 }, { id: ins:5, n: 5, hits: 0 }, { id: ins:3, n: 33, hits: 0 }] ON DUPLICATE KEY UPDATE hits += 1, n = $input.n;
INSERT INTO ins { id: ins:4, n: 44, hits: 0 } ON DUPLICATE KEY UPDATE hits += 7 RETURN BEFORE;
INSERT INTO ins [{ id: ins:9, n: 90, hits: 0 }, { id: ins:10, n: 'bad', hits: 0 }];
SELECT VALUE [record::id(id), n, hits] FROM ins ORDER BY id;
SELECT VALUE id FROM ins WHERE n > 12 ORDER BY id;
UPDATE ins SET hits += 1 WHERE n > 12;
UPSERT ins:6 SET hits += 10 RETURN BEFORE;
CREATE ins:7 SET n = 16, hits = 0;
SELECT VALUE id FROM ins WHERE n = 16 ORDER BY id;
-- @check
SELECT VALUE id FROM ins WHERE n = 14 ORDER BY id;
-- @drain
SELECT VALUE [record::id(id), n, hits] FROM ins ORDER BY id;
SELECT VALUE id FROM ins WHERE n = 16 ORDER BY id;
