-- A link the earlier release stored as a string becomes a record link under a migration. The cast keeps the
-- ids that name the field's table, fails over to the fallback for the rest, and never lets a link that
-- names another table stand as the field's record. A link stored as the text of a tuple-keyed record id
-- takes the same conversion. The migrated links read through FETCH, a path, an index and a write the way
-- links written at level 4 do.
DEFINE TABLE person SCHEMAFULL;
DEFINE FIELD name ON person TYPE string;
DEFINE TABLE post SCHEMAFULL;
DEFINE FIELD by ON post TYPE string;
CREATE person:1 SET name = 'Ann' RETURN NONE;
CREATE person:abc SET name = 'Bob' RETURN NONE;
CREATE person:⟨a b⟩ SET name = 'Cy' RETURN NONE;
CREATE other:1 SET name = 'Other' RETURN NONE;
CREATE post:1 SET by = 'person:1' RETURN NONE;
CREATE post:2 SET by = 'person:abc' RETURN NONE;
CREATE post:3 SET by = 'other:1' RETURN NONE;
CREATE post:4 SET by = 'nobody' RETURN NONE;
CREATE post:5 SET by = 'person:9' RETURN NONE;
CREATE post:6 SET by = 'person:⟨a b⟩' RETURN NONE;
DEFINE TABLE reading SCHEMAFULL;
DEFINE FIELD id ON reading TYPE [string, datetime];
DEFINE FIELD temp ON reading TYPE string;
CREATE reading:['London', d'2024-01-01T00:00:00Z'] SET temp = '11' RETURN NONE;
CREATE reading:['Rome', d'2024-01-01T00:00:00Z'] SET temp = '9' RETURN NONE;
DEFINE TABLE trip SCHEMAFULL;
DEFINE FIELD stop ON trip TYPE string;
CREATE trip:1 SET stop = "reading:['London', d'2024-01-01T00:00:00Z']" RETURN NONE;
CREATE trip:2 SET stop = "reading:['Rome', d'2024-01-01T00:00:00Z']" RETURN NONE;
CREATE trip:3 SET stop = "reading:['Paris', d'2024-01-01T00:00:00Z']" RETURN NONE;
CREATE trip:4 SET stop = 'reading:nope' RETURN NONE;
-- @upgrade
CREATE post:7 SET by = 'person:1' RETURN NONE;
ALTER FIELD by ON post TYPE option<record<person>> MIGRATE EXISTING ROWS AS by_rec USING <record<person>>$value FALLBACK NONE;
ALTER FIELD temp ON reading TYPE int MIGRATE EXISTING ROWS AS temp_x10 USING (<int>$value) * 10 FALLBACK -1;
ALTER FIELD stop ON trip TYPE option<record<reading>> MIGRATE EXISTING ROWS AS stop_rec USING <record<reading>>$value FALLBACK NONE;
SELECT VALUE by FROM post ORDER BY id;
SELECT VALUE by.name FROM post ORDER BY id;
SELECT * FROM post ORDER BY id FETCH by;
SELECT VALUE id FROM post WHERE by.name = 'Ann' ORDER BY id;
SELECT VALUE stop FROM trip ORDER BY id;
SELECT VALUE stop.temp FROM trip ORDER BY id;
SELECT * FROM trip ORDER BY id FETCH stop;
SELECT VALUE id FROM trip WHERE stop.temp > 50 ORDER BY id;
(INFO FOR TABLE post).migrations;
-- @check
DEFINE INDEX by_by ON post FIELDS by;
SELECT VALUE id FROM post WHERE by = person:1 ORDER BY id;
SELECT VALUE id FROM post WHERE by IS NONE ORDER BY id;
SELECT VALUE id FROM post WHERE by = other:1 ORDER BY id;
DEFINE INDEX by_stop ON trip FIELDS stop;
SELECT VALUE id FROM trip WHERE stop = reading:['London', d'2024-01-01T00:00:00Z'] ORDER BY id;
SELECT VALUE id FROM trip WHERE stop = reading:['Rome', d'2024-01-01T00:00:00Z'] ORDER BY id;
-- @drain
SELECT VALUE by FROM post ORDER BY id;
UPDATE post:4 SET by = other:1 RETURN AFTER;
UPDATE post:4 SET by = person:1 RETURN AFTER;
SELECT VALUE by FROM post ORDER BY id;
SELECT VALUE id FROM post WHERE by = person:1 ORDER BY id;
SELECT VALUE stop.temp FROM trip ORDER BY id;
UPDATE reading:['Rome', d'2024-01-01T00:00:00Z'] SET temp = 4 RETURN AFTER;
SELECT VALUE stop.temp FROM trip ORDER BY id;
-- @check
