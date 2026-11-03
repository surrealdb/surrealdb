/**
[env]
namespace = true
database = true

[test]
reason = "A domain's constant DEFAULT, signed or not, is stored as the value its base type is assigned: a number or boolean to text is its text, a numeric to an integer rounds half away from zero, a text to a number or boolean is read as one, and a numeric's scale rounds it"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: '1', b: 'true', c: 3i32, d: -1i32, e: -1.24dec, f: 1f32, g: true, h: 7i16, id: 1 }]"

[[test.results]]
value = "[{ a: '1', b: 'true', c: 3i32, d: -1i32, e: -1.24dec, f: 1f32, g: true, h: 7i16 }]"

[[test.results]]
value = "[{ a: 'text', c: 'integer', e: 'numeric', h: 'smallint' }]"

*/
CREATE DOMAIN d_num_text AS text DEFAULT 1;
CREATE DOMAIN d_bool_text AS text DEFAULT true;
CREATE DOMAIN d_num_int AS integer DEFAULT 2.5;
CREATE DOMAIN d_neg_int AS integer DEFAULT -1;
CREATE DOMAIN d_neg_num AS numeric(5,2) DEFAULT -1.235;
CREATE DOMAIN d_real AS real DEFAULT 1;
CREATE DOMAIN d_text_bool AS boolean DEFAULT 'yes';
CREATE DOMAIN d_text_small AS smallint DEFAULT '7';
CREATE TABLE t (id int PRIMARY KEY, a d_num_text, b d_bool_text, c d_num_int, d d_neg_int, e d_neg_num, f d_real, g d_text_bool, h d_text_small);
INSERT INTO t (id) VALUES (1);
SELECT a, b, c, d, e, f, g, h FROM t;
SELECT pg_typeof(a) AS a, pg_typeof(c) AS c, pg_typeof(e) AS e, pg_typeof(h) AS h FROM t;
