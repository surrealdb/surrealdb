-- An event writes into a second table whose field has a migration pending. An event write the
-- field's type refuses fails the statement that fired it, so that statement's own writes and
-- those of the events it fired are undone together. Inside a caller's transaction the later
-- statements read what the failed statement wrote before it failed, and that write refuses the
-- transaction's commit, so the transaction leaves nothing.
DEFINE TABLE src SCHEMAFULL;
DEFINE FIELD label ON src TYPE string;
DEFINE FIELD amt ON src TYPE string;
DEFINE TABLE dst SCHEMAFULL;
DEFINE FIELD total ON dst TYPE string;
DEFINE FIELD note ON dst TYPE option<string>;
CREATE dst:1 SET total = '10';
CREATE src:1 SET label = 'a', amt = '5';
-- @upgrade
ALTER FIELD total ON dst TYPE int MIGRATE EXISTING ROWS AS dtot USING <int>$value FALLBACK 0;
DEFINE EVENT feed ON src WHEN $event != 'DELETE' THEN (UPDATE dst:1 SET total = <int>$after.amt, note = $after.label);
UPDATE src:1 SET amt = '7';
SELECT * FROM dst ORDER BY id;
SELECT * FROM src ORDER BY id;
DEFINE EVENT strict ON src WHEN $event = 'UPDATE' THEN (UPDATE dst:1 SET total = $after.label);
UPDATE src:1 SET label = 'b';
SELECT * FROM dst ORDER BY id;
SELECT * FROM src ORDER BY id;
-- @drain
REMOVE EVENT strict ON src;
UPDATE src:1 SET label = 'b';
SELECT * FROM dst ORDER BY id;
SELECT * FROM src ORDER BY id;
-- @client
DEFINE EVENT strict ON src WHEN $event = 'UPDATE' THEN (UPDATE dst:1 SET total = $after.label);
UPDATE src:1 SET label = 'c';
SELECT * FROM dst ORDER BY id;
SELECT * FROM src ORDER BY id;
-- @end
-- @check
