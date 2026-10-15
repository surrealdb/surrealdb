-- Booleans and record links retyped on a populated table. A mixed legacy flag
-- (int, string, NULL, NONE, float) becomes bool through a comparison, and a bool
-- becomes int through a conditional. A string becomes record<person> through a
-- record cast: a string naming no person record, a link to another table, a
-- number, NULL and NONE take the FALLBACK, and a record<person> becomes string
-- through a cast. Probes the absent flag row (does it acquire the FALLBACK value)
-- and that an index on each migrated field agrees with a scan. An index that
-- reads a field blocks its migration, so it is removed before each migration.
DEFINE TABLE fl SCHEMAFULL;
DEFINE FIELD flag ON fl TYPE any;
DEFINE FIELD who ON fl TYPE any;
DEFINE FIELD name ON fl TYPE string;
CREATE fl:1 SET flag = 1, who = 'person:1', name = 'a';
CREATE fl:2 SET flag = 0, who = 'person:two', name = 'b';
CREATE fl:3 SET flag = 2, who = 'animal:9', name = 'c';
CREATE fl:4 SET flag = 'yes', who = NULL, name = 'd';
CREATE fl:5 SET flag = NULL, who = 1, name = 'e';
CREATE fl:6 SET name = 'f';
CREATE fl:7 SET flag = true, who = person:7, name = 'g';
CREATE fl:8 SET flag = 1.5, who = '', name = 'h';
SELECT VALUE flag FROM fl ORDER BY id;
SELECT VALUE who FROM fl ORDER BY id;
-- @upgrade
ALTER FIELD flag ON fl TYPE bool MIGRATE EXISTING ROWS AS flag_bool USING $value != 0;
ALTER FIELD flag ON fl TYPE bool MIGRATE EXISTING ROWS AS flag_bool USING $value != 0 FALLBACK false;
ALTER FIELD who ON fl TYPE record<person> MIGRATE EXISTING ROWS AS who_rec USING <record<person>>$value;
ALTER FIELD who ON fl TYPE record<person> MIGRATE EXISTING ROWS AS who_rec USING <record<person>>$value FALLBACK person:0;
SELECT VALUE flag FROM fl ORDER BY id;
SELECT VALUE who FROM fl ORDER BY id;
SELECT VALUE flag FROM ONLY fl:6;
(INFO FOR TABLE fl).migrations.guaranteed;
-- @drain
(INFO FOR TABLE fl).migrations.guaranteed;
DEFINE INDEX by_who ON fl FIELDS who;
DEFINE INDEX by_flag ON fl FIELDS flag;
-- @check
SELECT VALUE name FROM fl WHERE who = person:0 ORDER BY id;
SELECT VALUE name FROM fl WHERE who = person:1 ORDER BY id;
SELECT VALUE name FROM fl WHERE flag = true ORDER BY id;
SELECT VALUE name FROM fl WHERE flag = false ORDER BY id;
-- @drain
SELECT VALUE flag FROM fl ORDER BY id;
-- Booleans back to int, with the index on the flag removed first.
REMOVE INDEX by_flag ON fl;
ALTER FIELD flag ON fl TYPE int MIGRATE EXISTING ROWS AS flag_int USING IF $value THEN 1 ELSE 0 END;
ALTER FIELD flag ON fl TYPE int MIGRATE EXISTING ROWS AS flag_int USING IF $value THEN 1 ELSE 0 END FALLBACK -1;
SELECT VALUE flag FROM fl ORDER BY id;
-- @check
-- Record links back to string, with the index on the link removed first.
REMOVE INDEX by_who ON fl;
ALTER FIELD who ON fl TYPE string MIGRATE EXISTING ROWS AS who_str USING <string>$value;
ALTER FIELD who ON fl TYPE string MIGRATE EXISTING ROWS AS who_str USING <string>$value FALLBACK 'none';
SELECT VALUE who FROM fl ORDER BY id;
DEFINE INDEX by_who ON fl FIELDS who;
SELECT VALUE name FROM fl WHERE who = 'none' ORDER BY id;
-- @drain
SELECT VALUE who FROM fl ORDER BY id;
(INFO FOR TABLE fl).migrations.generation;
-- Rows written after the changes: a record link is refused in a string field,
-- and a bool is refused in an int field.
CREATE fl:9 SET flag = 5, who = person:9, name = 'i';
CREATE fl:10 SET flag = true, who = 'person:10', name = 'j';
SELECT VALUE who FROM fl ORDER BY id;
-- @check
