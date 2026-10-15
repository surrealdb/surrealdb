-- In-place upgrade with every scalar kind a 3.x store holds. Typed values (int, float,
-- decimal, number, a datetime before the epoch, duration, uuid, bytes, point geometry,
-- record link, unicode text) and mixed-type values under one indexed field are written
-- before @upgrade. Indexed point and range lookups over them must answer after the upgrade
-- as a scan does, and updating or deleting a legacy row must move or drop its index entry.
DEFINE TABLE typed SCHEMAFULL;
DEFINE FIELD i ON typed TYPE int;
DEFINE FIELD fl ON typed TYPE float;
DEFINE FIELD dc ON typed TYPE decimal;
DEFINE FIELD num ON typed TYPE number;
DEFINE FIELD ts ON typed TYPE datetime;
DEFINE FIELD dur ON typed TYPE duration;
DEFINE FIELD uid ON typed TYPE uuid;
DEFINE FIELD raw ON typed TYPE bytes;
DEFINE FIELD pos ON typed TYPE geometry<point>;
DEFINE FIELD who ON typed TYPE option<record<person>>;
DEFINE FIELD txt ON typed TYPE string;
DEFINE INDEX t_i ON typed FIELDS i;
DEFINE INDEX t_ts ON typed FIELDS ts;
DEFINE INDEX t_dur ON typed FIELDS dur;
DEFINE INDEX t_uid ON typed FIELDS uid;
DEFINE INDEX t_who ON typed FIELDS who;
CREATE typed:1 SET i = -7, fl = 1.5f, dc = 0.1dec, num = 2, ts = d'1960-06-15T00:00:00Z', dur = 1h30m, uid = u'018f3c7e-0000-7000-8000-000000000001', raw = b"0102ff", pos = (1.5, -2.25), who = person:tobie, txt = 'naïve café ✓ 日本語 🚀';
CREATE typed:2 SET i = 0, fl = 0.0f, dc = -2.50dec, num = 2.5f, ts = d'1970-01-01T00:00:00Z', dur = 500ms, uid = u'018f3c7e-0000-7000-8000-000000000002', raw = b"", pos = (0, 0), txt = '';
CREATE typed:3 SET i = 9223372036854775807, fl = -1.25f, dc = 1.10dec, num = -1, ts = d'2024-02-29T12:34:56.789Z', dur = 2w3d, uid = u'018f3c7e-0000-7000-8000-000000000003', raw = <bytes>'hi', pos = (-179.5, 89.5), who = person:jaime, txt = 'it\'s "quoted"';
CREATE typed:4 SET i = 3, fl = 2.0f, dc = 0.25dec, num = 3dec, ts = d'1969-12-31T23:59:59.999Z', dur = 1ns, uid = u'018f3c7e-0000-7000-8000-000000000004', raw = b"ff", pos = (10, 20), txt = 'Ωmega';
CREATE typed:5 SET i = -1, fl = -0.5f, dc = 7dec, num = 1.5f, ts = d'2000-01-01T00:00:00Z', dur = 0ns, uid = u'018f3c7e-0000-7000-8000-000000000005', raw = b"", pos = (-1, -1), who = person:tobie, txt = 'mixed';
DEFINE TABLE kinds SCHEMALESS;
DEFINE INDEX k_v ON kinds FIELDS v;
CREATE kinds:1 SET v = 1;
CREATE kinds:2 SET v = 1.0f;
CREATE kinds:3 SET v = 1dec;
CREATE kinds:4 SET v = '1';
CREATE kinds:5 SET v = [1];
CREATE kinds:6 SET v = { a: 1 };
CREATE kinds:7 SET v = NULL;
CREATE kinds:8 SET v = NONE;
CREATE kinds:9 SET v = true;
CREATE kinds:10 SET v = d'1970-01-01T00:00:00Z';
CREATE kinds:11 SET v = 1h;
CREATE kinds:12 SET v = u'018f3c7e-0000-7000-8000-000000000002';
CREATE kinds:13 SET v = (1, 1);
CREATE kinds:14 SET v = person:tobie;
CREATE kinds:15 SET v = b"01";
CREATE kinds:16 SET v = -1;
CREATE kinds:17 SET v = 1.5f;
-- @upgrade
SELECT * FROM typed ORDER BY id;
SELECT VALUE id FROM typed WHERE i = 3 ORDER BY id;
SELECT VALUE id FROM typed WHERE i > -1 ORDER BY i;
SELECT VALUE id FROM typed WHERE i < 0 ORDER BY i;
SELECT VALUE id FROM typed WHERE ts < d'1970-01-01T00:00:00Z' ORDER BY ts;
SELECT VALUE id FROM typed WHERE ts >= d'1969-12-31T23:59:59.999Z' ORDER BY ts;
SELECT VALUE id FROM typed WHERE dur > 1h ORDER BY dur;
SELECT VALUE id FROM typed WHERE dur = 0ns ORDER BY id;
SELECT VALUE id FROM typed WHERE uid = u'018f3c7e-0000-7000-8000-000000000002';
SELECT VALUE id FROM typed WHERE who = person:tobie ORDER BY id;
SELECT VALUE id FROM kinds WHERE v = 1 ORDER BY id;
SELECT VALUE id FROM kinds WHERE v = '1' ORDER BY id;
SELECT VALUE id FROM kinds WHERE v = NULL ORDER BY id;
SELECT VALUE id FROM kinds WHERE v > 0 ORDER BY id;
SELECT VALUE v FROM kinds ORDER BY v;
-- @check
UPDATE typed:1 SET i = 1000;
SELECT VALUE id FROM typed WHERE i = -7 ORDER BY id;
DELETE typed:3;
SELECT VALUE id FROM typed WHERE i = 9223372036854775807 ORDER BY id;
UPDATE kinds:1 SET v = 'changed';
SELECT VALUE id FROM kinds WHERE v = 1 ORDER BY id;
-- @drain
SELECT * FROM typed ORDER BY id;
SELECT VALUE id FROM kinds WHERE v = 1.0 ORDER BY id;
-- @check
CREATE typed:6 SET i = -100, fl = 3.0f, dc = 1dec, num = 0, ts = d'1900-01-01T00:00:00Z', dur = 3d, uid = u'018f3c7e-0000-7000-8000-000000000006', raw = b"00", pos = (0, 1), who = person:ann, txt = 'new row';
SELECT VALUE id FROM typed WHERE ts < d'1970-01-01T00:00:00Z' ORDER BY ts;
SELECT VALUE id FROM typed WHERE i < 0 ORDER BY i;
SELECT * FROM typed ORDER BY id;
-- @check
