-- A parent removed while its child's step is pending: level 3 keeps the removed values, level 4 strips them.
DEFINE TABLE rp SCHEMAFULL;
DEFINE FIELD o ON rp TYPE option<object>;
DEFINE FIELD o.n ON rp TYPE option<string>;
CREATE rp:1 SET o = { n: '1' } RETURN NONE;
-- @upgrade
ALTER FIELD o.n ON rp TYPE option<int> MIGRATE EXISTING ROWS AS n_int USING <int>$value + 1000 FALLBACK -1;
REMOVE FIELD o ON rp;
SELECT * FROM rp;
DEFINE FIELD o ON rp TYPE option<string>;
SELECT * FROM rp;
-- @drain
SELECT * FROM rp;
