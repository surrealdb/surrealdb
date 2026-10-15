-- Reads of a row behind a step inside ON DUPLICATE KEY UPDATE and UPDATE SET.
DEFINE TABLE q SCHEMAFULL;
DEFINE FIELD n ON q TYPE string;
DEFINE FIELD prev ON q TYPE any;
DEFINE FIELD prev2 ON q TYPE any;
CREATE q:1 SET n = '1';
CREATE q:2 SET n = '2';
ALTER FIELD n ON q TYPE int MIGRATE EXISTING ROWS AS n_plus USING <int>$value + 10 FALLBACK -1;
INSERT INTO q { id: q:1, n: 0 } ON DUPLICATE KEY UPDATE prev = n, prev2 = $before.n;
UPDATE q:2 SET prev = n, prev2 = $before.n;
INSERT INTO q { id: q:2, n: 0 } ON DUPLICATE KEY UPDATE n = (n ?? 0) + 100;
SELECT VALUE [record::id(id), n, prev, prev2] FROM q ORDER BY id;
