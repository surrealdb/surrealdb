-- A range on the leading column of a composite index: an exclusive bound
-- excludes every key that begins with the bound value, whatever follows it.
DEFINE TABLE tc SCHEMAFULL;
DEFINE FIELD g ON tc TYPE string;
DEFINE FIELD a ON tc TYPE int;
CREATE tc:1 SET g = 'x', a = 5;
CREATE tc:2 SET g = 'y', a = 5;
CREATE tc:3 SET g = 'y', a = 6;
DEFINE INDEX ug ON tc FIELDS g, a;
SELECT VALUE id FROM tc WHERE g > 'x' ORDER BY id;
SELECT VALUE id FROM tc WHERE g < 'y' ORDER BY id;
SELECT VALUE id FROM tc WHERE g >= 'y' ORDER BY id;
SELECT VALUE id FROM tc WHERE g <= 'x' ORDER BY id;
-- @check
