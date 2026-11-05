-- Property: a view reads its source tables however the definition names them. A view
-- over two FROM tables holds a migration on either, and defining it while both have
-- rows behind a step either succeeds or is refused with a reason. A view whose value is
-- read from another table by a subquery, or through a record link, is a dependency of
-- that table: a migration of the table it reads is refused, or the view keeps the value it
-- read and no migration changes that value underneath it. The last part repeats the
-- subquery and record-link reads with no migration anywhere, as a control, on tables of
-- their own.
DEFINE TABLE pa SCHEMAFULL;
DEFINE FIELD v ON pa TYPE string;
DEFINE TABLE pb SCHEMAFULL;
DEFINE FIELD v ON pb TYPE string;
DEFINE FIELD ref ON pb TYPE option<record<pa>>;
CREATE pa:1 SET v = '1' RETURN NONE;
CREATE pa:2 SET v = 'x' RETURN NONE;
CREATE pb:1 SET v = '2', ref = pa:1 RETURN NONE;
CREATE pb:2 SET v = '3', ref = pa:2 RETURN NONE;
-- @upgrade
ALTER FIELD v ON pa TYPE int MIGRATE EXISTING ROWS AS va_int USING <int>$value FALLBACK -1;
DEFINE TABLE both AS SELECT v FROM pa, pb;
SELECT * FROM both ORDER BY v;
ALTER FIELD v ON pb TYPE int MIGRATE EXISTING ROWS AS vb_int USING <int>$value FALLBACK -1;
REMOVE TABLE both;
ALTER FIELD v ON pb TYPE int MIGRATE EXISTING ROWS AS vb_int USING <int>$value FALLBACK -1;
DEFINE TABLE sub AS SELECT id, (SELECT VALUE v FROM ONLY pa:1) AS pv FROM pb;
SELECT * FROM sub ORDER BY id;
-- @check
ALTER FIELD v ON pa TYPE int MIGRATE EXISTING ROWS AS va_plus USING $value + 1 FALLBACK 0;
SELECT VALUE v FROM ONLY pa:1;
SELECT * FROM sub ORDER BY id;
-- @drain
SELECT VALUE v FROM ONLY pa:1;
SELECT * FROM sub ORDER BY id;
SELECT VALUE ref.v FROM pb ORDER BY id;
-- @check
DEFINE TABLE linked AS SELECT id, ref.v AS rv FROM pb;
SELECT * FROM linked ORDER BY id;
ALTER FIELD v ON pa TYPE int MIGRATE EXISTING ROWS AS va_x3 USING $value * 3 FALLBACK 0;
SELECT VALUE v FROM ONLY pa:1;
SELECT * FROM linked ORDER BY id;
SELECT * FROM sub ORDER BY id;
-- @check
-- Control, no migration: a view over two tables whose record ids overlap.
DEFINE TABLE qa SCHEMAFULL;
DEFINE FIELD v ON qa TYPE string;
DEFINE TABLE qb SCHEMAFULL;
DEFINE FIELD v ON qb TYPE string;
CREATE qa:1 SET v = '1' RETURN NONE;
CREATE qb:1 SET v = '2' RETURN NONE;
DEFINE TABLE qboth AS SELECT v FROM qa, qb;
-- Control, no migration: a view whose value is read from another table by a subquery.
DEFINE TABLE ka SCHEMAFULL;
DEFINE FIELD v ON ka TYPE int;
DEFINE TABLE kb SCHEMAFULL;
DEFINE FIELD v ON kb TYPE int;
CREATE ka:1 SET v = 1 RETURN NONE;
CREATE kb:1 SET v = 2 RETURN NONE;
DEFINE TABLE ksub AS SELECT id, (SELECT VALUE v FROM ONLY ka:1) AS pv FROM kb;
SELECT * FROM ksub ORDER BY id;
UPDATE ka:1 SET v = 2 RETURN NONE;
SELECT * FROM ksub ORDER BY id;
SELECT VALUE v FROM ONLY ka:1;
-- Control, no migration: a view that reads another table through a record link.
DEFINE TABLE la SCHEMAFULL;
DEFINE FIELD v ON la TYPE int;
DEFINE TABLE lb SCHEMAFULL;
DEFINE FIELD ref ON lb TYPE option<record<la>>;
CREATE la:1 SET v = 1 RETURN NONE;
CREATE lb:1 SET ref = la:1 RETURN NONE;
DEFINE TABLE llinked AS SELECT id, ref.v AS rv FROM lb;
SELECT * FROM llinked ORDER BY id;
UPDATE la:1 SET v = 6 RETURN NONE;
SELECT * FROM llinked ORDER BY id;
SELECT VALUE ref.v FROM lb ORDER BY id;
-- @check
