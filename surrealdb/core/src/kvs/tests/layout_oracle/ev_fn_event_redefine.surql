-- User functions called from an event and from a computed field of a table with a migration
-- pending. The job rewrites rows without running either, so a read and an event see the body
-- current at that moment, the same in lazy and eager runs. A function removed while an event
-- calls it leaves the writes that fire the event failing, and no partial write survives them; the
-- computed field that calls it goes first, so the table's reads do not depend on the function.
DEFINE FUNCTION fn::label($v: any) { RETURN 'v1:' + <string>$v };
DEFINE TABLE tb SCHEMAFULL;
DEFINE FIELD n ON tb TYPE string;
DEFINE FIELD shown ON tb COMPUTED fn::label(n);
DEFINE TABLE log SCHEMALESS;
DEFINE EVENT lbl ON tb WHEN $event = 'CREATE' THEN (CREATE log SET txt = fn::label($after.n));
CREATE tb:1 SET n = '1';
CREATE tb:2 SET n = 'x';
-- @upgrade
ALTER FIELD n ON tb TYPE int MIGRATE EXISTING ROWS AS m USING <int>$value FALLBACK 0;
SELECT VALUE shown FROM tb ORDER BY id;
DEFINE FUNCTION OVERWRITE fn::label($v: any) { RETURN 'v2:' + <string>$v };
SELECT VALUE shown FROM tb ORDER BY id;
CREATE tb:3 SET n = 3;
SELECT VALUE txt FROM log ORDER BY txt;
-- @drain
SELECT VALUE shown FROM tb ORDER BY id;
SELECT VALUE txt FROM log ORDER BY txt;
REMOVE FIELD shown ON tb;
REMOVE FUNCTION fn::label;
CREATE tb:4 SET n = 4;
SELECT VALUE id FROM tb ORDER BY id;
SELECT VALUE txt FROM log ORDER BY txt;
-- @check
