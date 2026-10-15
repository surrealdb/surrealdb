-- SET, MERGE, PATCH, CONTENT, REPLACE and UPSERT on rows still behind a pending step:
-- each write reads the migrated value of the fields it does not name, stores that
-- value once, and stores the values it names as written. A rejected write leaves its
-- row as the steps left it, and an index defined over pending rows follows the writes.
DEFINE TABLE w SCHEMAFULL;
DEFINE FIELD n ON w TYPE string;
DEFINE FIELD tag ON w TYPE option<string>;
DEFINE FIELD note ON w TYPE option<string>;
CREATE w:1 SET n = '1', tag = 'a';
CREATE w:2 SET n = '2', tag = 'b';
CREATE w:3 SET n = '3', tag = 'c';
CREATE w:4 SET n = '4', tag = 'd';
CREATE w:5 SET n = '5', tag = 'e';
CREATE w:6 SET n = '6', tag = 'f';
CREATE w:7 SET n = '7', tag = 'g';
-- @upgrade
ALTER FIELD n ON w TYPE int MIGRATE EXISTING ROWS AS n_plus USING <int>$value + 10 FALLBACK -1;
DEFINE INDEX by_n ON w FIELDS n;
UPDATE w:1 SET note = 'x' RETURN BEFORE;
UPDATE w:2 MERGE { tag: 'z' } RETURN BEFORE;
UPDATE w:3 PATCH [{ op: 'replace', path: '/tag', value: 'p' }] RETURN DIFF;
UPDATE w:4 CONTENT { n: 40, tag: 'q' } RETURN BEFORE;
UPDATE w:5 REPLACE { n: 50 } RETURN BEFORE;
UPSERT w:6 SET tag = 'u' RETURN BEFORE;
UPSERT w:8 SET n = 8, tag = 'new';
UPDATE w:7 SET n = '70';
UPDATE w:7 CONTENT { tag: 'bad' };
SELECT VALUE [record::id(id), n, tag, note] FROM w ORDER BY id;
SELECT VALUE id FROM w WHERE n = 12 ORDER BY id;
-- @check
UPDATE w SET note = 'all' WHERE n > 14;
SELECT VALUE id FROM w WHERE n > 14 ORDER BY id;
-- @drain
SELECT VALUE [record::id(id), n, tag, note] FROM w ORDER BY id;
SELECT VALUE id FROM w WHERE n = 16 ORDER BY id;
