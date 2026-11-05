-- A SCHEMALESS table keeps what its rows store. REMOVE FIELD mints no removal step there, so the
-- values of a removed name stay in the rows; writes to the removed name in between, an index over
-- the name, and a re-declared TYPE that the stored values contradict must all read the same by index,
-- by scan and by filter. Once the index is removed, a removed and re-declared name reads what the
-- rows hold, and nothing else.
DEFINE TABLE loose SCHEMALESS;
DEFINE FIELD n ON loose TYPE int;
DEFINE INDEX by_n ON loose FIELDS n;
CREATE loose:1 SET n = 1, m = 'a' RETURN NONE;
CREATE loose:2 SET n = 2 RETURN NONE;
-- @upgrade
CREATE loose:3 SET n = 3 RETURN NONE;
REMOVE FIELD n ON loose;
SELECT * FROM loose ORDER BY id;
SELECT VALUE id FROM loose WHERE n = 2;
-- Written to the removed name, with a value of another type.
CREATE loose:4 SET n = 'four', m = 'b' RETURN NONE;
UPDATE loose:1 SET n = 10 RETURN NONE;
SELECT * FROM loose ORDER BY id;
SELECT VALUE id FROM loose WHERE n = 'four';
-- @check
-- Re-declared as a string: the int values stored before the removal are still there.
DEFINE FIELD n ON loose TYPE option<string>;
SELECT * FROM loose ORDER BY id;
SELECT VALUE id FROM loose WHERE n = 'four';
SELECT VALUE id FROM loose WHERE n = 2;
SELECT VALUE id FROM loose WHERE n = 10;
-- @check
-- @drain
SELECT * FROM loose ORDER BY id;
(INFO FOR TABLE loose).migrations;
-- The index and the field removed, then re-declared as an int with the index again.
REMOVE INDEX by_n ON loose;
REMOVE FIELD n ON loose;
DEFINE FIELD n ON loose TYPE option<int>;
SELECT * FROM loose ORDER BY id;
DEFINE INDEX by_n2 ON loose FIELDS n;
SELECT VALUE id FROM loose WHERE n = 'four';
SELECT VALUE id FROM loose WHERE n = 3;
-- A MIGRATE on a SCHEMALESS removed-then-redeclared field repairs the rows it names.
REMOVE INDEX by_n2 ON loose;
ALTER FIELD n ON loose TYPE int MIGRATE EXISTING ROWS AS n_int USING <int>$value FALLBACK -1;
DEFINE INDEX by_n3 ON loose FIELDS n;
SELECT * FROM loose ORDER BY id;
-- @check
-- @drain
SELECT * FROM loose ORDER BY id;
SELECT VALUE id FROM loose WHERE n = -1 ORDER BY id;
SELECT VALUE id FROM loose WHERE n = 3 ORDER BY id;
SELECT VALUE id FROM loose WHERE n = 'four' ORDER BY id;
(INFO FOR TABLE loose).migrations;
-- @check
