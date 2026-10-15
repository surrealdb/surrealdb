-- A field's type changes under a migration while rows written before it,
-- some by the earlier release, stay behind until the job reaches them.
DEFINE TABLE item SCHEMAFULL;
DEFINE FIELD n ON item TYPE string;
DEFINE FIELD tag ON item TYPE option<string>;
CREATE item:1 SET n = '1', tag = 'a';
CREATE item:2 SET n = '2';
CREATE item:3 SET n = 'x', tag = 'b';
-- @upgrade
CREATE item:4 SET n = '4', tag = 'a';
ALTER FIELD n ON item TYPE int MIGRATE EXISTING ROWS AS n_to_int USING <int>$value FALLBACK -1;
CREATE item:5 SET n = 5;
SELECT * FROM item ORDER BY id;
SELECT VALUE id FROM item WHERE n = -1;
-- @check
DEFINE INDEX by_n ON item FIELDS n;
SELECT VALUE id FROM item WHERE n > 1 ORDER BY id;
-- @drain
SELECT * FROM item ORDER BY id;
