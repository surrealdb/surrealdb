/**
[env]
namespace = true
database = true

[test]
reason = "Rows of a SCHEMAFULL table are packed; a read of each kind of column in a filter, an aggregate, an ordering and a projection answers what the whole decode of the row holds."

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
value = """[{ b: true, d: 2.25dec, du: 1m30s, f: 1.5f, i: 1, id: kind:1, link: kind:2, lit: 'red', m: 1, n: 3, o: 7, pt: (1f, 2f), s: 'one', t: d'2024-01-02T03:04:05Z', tags: ['x', 'y'], u: u'00000000-0000-0000-0000-000000000001', y: b"6162" }]"""

[[test.results]]
value = "[{ b: false, d: 0dec, du: 600y, f: -0.5f, i: -9223372036854775807, id: kind:2, lit: 'green', m: NULL, n: NULL, s: '', t: d'9999-01-01T00:00:00Z', tags: [], u: u'ffffffff-ffff-ffff-ffff-ffffffffffff' }]"

[[test.results]]
value = """[{ b: true, d: 3dec, du: 0ns, f: 3f, i: 3, id: kind:3, lit: 'blue', n: 4, s: 'a string longer than twelve bytes', t: d'1000-01-01T00:00:00Z', tags: ['a'], u: u'00000000-0000-0000-0000-000000000003', y: b"" }]"""

[[test.results]]
value = "[{ b: false, d: 4dec, du: 1h, f: 4.25f, i: 4, id: kind:4, lit: 'red', m: 4, n: NULL, o: 11, pt: (-3.5f, 0f), s: 'four', t: d'2001-02-03T04:05:06Z', tags: ['q'], u: u'00000000-0000-0000-0000-000000000004' }]"

[[test.results]]
value = "[{ b: true, d: 5.5dec, du: 2h30m, f: 5.5f, i: 5, id: kind:5, lit: 'green', n: 5, s: 'five', t: d'2002-02-03T04:05:06.123456789Z', tags: ['q', 'r'], u: u'00000000-0000-0000-0000-000000000005' }]"

[[test.results]]
value = "[{ b: false, d: 6dec, du: 3h, f: 6f, i: 6, id: kind:6, lit: 'blue', m: NULL, n: 6, o: 0, s: 'six', t: d'2003-02-03T04:05:06Z', tags: ['s'], u: u'00000000-0000-0000-0000-000000000006' }]"

[[test.results]]
value = "[{ b: true, d: 7dec, du: 4h, f: 7f, i: 7, id: kind:7, link: kind:1, lit: 'red', n: 7, o: 8, s: 'seven', t: d'2004-02-03T04:05:06Z', tags: [], u: u'00000000-0000-0000-0000-000000000007' }]"

[[test.results]]
value = "[{ b: false, d: 8dec, du: 5h, f: 8f, i: 8, id: kind:8, lit: 'green', n: 8, pt: (1f, 2f), s: 'eight', t: d'2005-02-03T04:05:06Z', tags: ['t', 'u'], u: u'00000000-0000-0000-0000-000000000008' }]"

[[test.results]]
value = """[{ b: true, d: 2.25dec, du: 1m30s, f: 1.5f, i: 1, id: kind:1, link: kind:2, lit: 'red', m: 1, n: 3, o: 7, pt: (1f, 2f), s: 'one', t: d'2024-01-02T03:04:05Z', tags: ['x', 'y'], u: u'00000000-0000-0000-0000-000000000001', y: b"6162" }, { b: false, d: 0dec, du: 600y, f: -0.5f, i: -9223372036854775807, id: kind:2, lit: 'green', m: NULL, n: NULL, s: '', t: d'9999-01-01T00:00:00Z', tags: [], u: u'ffffffff-ffff-ffff-ffff-ffffffffffff' }, { b: true, d: 3dec, du: 0ns, f: 3f, i: 3, id: kind:3, lit: 'blue', n: 4, s: 'a string longer than twelve bytes', t: d'1000-01-01T00:00:00Z', tags: ['a'], u: u'00000000-0000-0000-0000-000000000003', y: b"" }, { b: false, d: 4dec, du: 1h, f: 4.25f, i: 4, id: kind:4, lit: 'red', m: 4, n: NULL, o: 11, pt: (-3.5f, 0f), s: 'four', t: d'2001-02-03T04:05:06Z', tags: ['q'], u: u'00000000-0000-0000-0000-000000000004' }, { b: true, d: 5.5dec, du: 2h30m, f: 5.5f, i: 5, id: kind:5, lit: 'green', n: 5, s: 'five', t: d'2002-02-03T04:05:06.123456789Z', tags: ['q', 'r'], u: u'00000000-0000-0000-0000-000000000005' }, { b: false, d: 6dec, du: 3h, f: 6f, i: 6, id: kind:6, lit: 'blue', m: NULL, n: 6, o: 0, s: 'six', t: d'2003-02-03T04:05:06Z', tags: ['s'], u: u'00000000-0000-0000-0000-000000000006' }, { b: true, d: 7dec, du: 4h, f: 7f, i: 7, id: kind:7, link: kind:1, lit: 'red', n: 7, o: 8, s: 'seven', t: d'2004-02-03T04:05:06Z', tags: [], u: u'00000000-0000-0000-0000-000000000007' }, { b: false, d: 8dec, du: 5h, f: 8f, i: 8, id: kind:8, lit: 'green', n: 8, pt: (1f, 2f), s: 'eight', t: d'2005-02-03T04:05:06Z', tags: ['t', 'u'], u: u'00000000-0000-0000-0000-000000000008' }]"""

[[test.results]]
value = "[{ count: 8 }]"

[[test.results]]
value = "[{ md: 8dec, mf: 4.34375f, si: -9223372036854775773 }]"

[[test.results]]
value = "[kind:8, kind:7, kind:6, kind:5, kind:4, kind:3, kind:1, kind:2]"

[[test.results]]
value = "[kind:2, kind:3, kind:8, kind:5, kind:4, kind:1, kind:7, kind:6]"

[[test.results]]
value = "[kind:8, kind:7, kind:6, kind:5, kind:4, kind:3, kind:1, kind:2]"

[[test.results]]
value = "[kind:2, kind:1, kind:3, kind:4, kind:5, kind:6, kind:7, kind:8]"

[[test.results]]
value = "[kind:2, kind:1, kind:8, kind:7, kind:6, kind:5, kind:4, kind:3]"

[[test.results]]
value = "[kind:3, kind:1, kind:4, kind:5, kind:6, kind:7, kind:8, kind:2]"

[[test.results]]
value = "[kind:2, kind:8, kind:7, kind:6, kind:5, kind:4, kind:3, kind:1]"

[[test.results]]
value = "[kind:2, kind:3, kind:5, kind:8, kind:6, kind:1, kind:7, kind:4]"

[[test.results]]
value = "[kind:2, kind:4, kind:1, kind:3, kind:5, kind:6, kind:7, kind:8]"

[[test.results]]
value = "[kind:3, kind:6, kind:2, kind:5, kind:8, kind:1, kind:4, kind:7]"

[[test.results]]
value = "[kind:2, kind:4, kind:6, kind:8, kind:1, kind:3, kind:5, kind:7]"

[[test.results]]
value = "[kind:2, kind:1, kind:3]"

[[test.results]]
value = "[{ b: false, d: 4dec, du: 1h, f: 4.25f, i: 4, id: kind:4, lit: 'red', m: 4, n: NULL, o: 11, pt: (-3.5f, 0f), s: 'four', t: d'2001-02-03T04:05:06Z', tags: ['q'], u: u'00000000-0000-0000-0000-000000000004' }]"

[[test.results]]
value = """[{ b: true, d: 2.25dec, du: 1m30s, f: 1.5f, i: 1, id: kind:1, link: kind:2, lit: 'red', m: 1, n: 3, o: 7, pt: (1f, 2f), s: 'one', t: d'2024-01-02T03:04:05Z', tags: ['x', 'y'], u: u'00000000-0000-0000-0000-000000000001', y: b"6162" }, { b: false, d: 0dec, du: 600y, f: -0.5f, i: -9223372036854775807, id: kind:2, lit: 'green', m: NULL, n: NULL, s: '', t: d'9999-01-01T00:00:00Z', tags: [], u: u'ffffffff-ffff-ffff-ffff-ffffffffffff' }, { b: true, d: 3dec, du: 0ns, f: 3f, i: 3, id: kind:3, lit: 'blue', n: 4, s: 'a string longer than twelve bytes', t: d'1000-01-01T00:00:00Z', tags: ['a'], u: u'00000000-0000-0000-0000-000000000003', y: b"" }]"""

[[test.results]]
value = "[kind:1, kind:3, kind:5, kind:7]"

[[test.results]]
value = "[kind:3, kind:4, kind:5, kind:6]"

[[test.results]]
value = "[kind:1, kind:3, kind:4, kind:5, kind:7]"

[[test.results]]
value = "[{ b: true, d: 5.5dec, du: 2h30m, f: 5.5f, i: 5, id: kind:5, lit: 'green', n: 5, s: 'five', t: d'2002-02-03T04:05:06.123456789Z', tags: ['q', 'r'], u: u'00000000-0000-0000-0000-000000000005' }]"

[[test.results]]
value = "[kind:2, kind:3, kind:8]"

[[test.results]]
value = "[kind:1, kind:6]"

[[test.results]]
value = "[kind:5]"

[[test.results]]
value = "[kind:2]"

[[test.results]]
value = "[kind:3, kind:4, kind:5, kind:6, kind:7, kind:8]"

[[test.results]]
value = "[kind:2]"

[[test.results]]
value = "[kind:3]"

[[test.results]]
value = "[kind:4, kind:5, kind:6, kind:7, kind:8]"

[[test.results]]
value = "[kind:5]"

[[test.results]]
value = "[kind:1, kind:3, kind:5, kind:7]"

[[test.results]]
value = "[kind:4, kind:6, kind:8]"

[[test.results]]
value = "[kind:1, kind:2, kind:5, kind:6, kind:7, kind:8]"

[[test.results]]
value = "[kind:5]"

[[test.results]]
value = "[kind:3]"

[[test.results]]
value = "[kind:2]"

[[test.results]]
value = "[kind:2, kind:5, kind:6, kind:7, kind:8]"

[[test.results]]
value = "[kind:1]"

[[test.results]]
value = "[kind:2]"

[[test.results]]
value = "[kind:4]"

[[test.results]]
value = "[kind:2, kind:6, kind:7, kind:8]"

[[test.results]]
value = "[kind:1]"

[[test.results]]
value = "[kind:2, kind:4, kind:5, kind:6, kind:7, kind:8]"

[[test.results]]
value = "[kind:1, kind:3]"

[[test.results]]
value = "[kind:2, kind:3, kind:5, kind:8]"

[[test.results]]
value = "[kind:1, kind:4, kind:6, kind:7]"

[[test.results]]
value = "[kind:6]"

[[test.results]]
value = "[kind:4, kind:7]"

[[test.results]]
value = "[kind:2, kind:4]"

[[test.results]]
value = "[kind:2, kind:4]"

[[test.results]]
value = "[kind:1, kind:3, kind:5, kind:6, kind:7, kind:8]"

[[test.results]]
value = "[kind:3, kind:5, kind:7, kind:8]"

[[test.results]]
value = "[kind:2, kind:6]"

[[test.results]]
value = "[kind:1]"

[[test.results]]
value = "[kind:4, kind:5]"

[[test.results]]
value = "[kind:1]"

[[test.results]]
value = "[kind:2, kind:7]"

[[test.results]]
value = "[kind:1]"

[[test.results]]
value = "[kind:2, kind:3, kind:4, kind:5, kind:6, kind:8]"

[[test.results]]
value = "[kind:1, kind:4, kind:7]"

[[test.results]]
value = "[kind:1, kind:3, kind:4, kind:6, kind:7]"

[[test.results]]
value = "[kind:1, kind:3, kind:4, kind:6, kind:7]"

[[test.results]]
value = "[kind:2, kind:3, kind:5, kind:6, kind:7]"

[[test.results]]
value = "[kind:1, kind:8]"

[[test.results]]
value = "[kind:1, kind:4, kind:8]"

[[test.results]]
value = "['four']"

[[test.results]]
value = "[1, 3, 5, 7]"

[[test.results]]
value = "[{ b: false, i: 6, s: 'six' }, { b: true, i: 7, s: 'seven' }, { b: false, i: 8, s: 'eight' }]"

[[test.results]]
value = "[{ dec_d: true, float_f: true, int_i: true }]"

[[test.results]]
value = "[[false, true, false, false], [false, false, false, true], [true, false, true, false]]"

[[test.results]]
value = "[{ i: 3, s: 'a string longer than twelve bytes' }]"

[[test.results]]
value = """[{ du: 1m30s, pt: (1f, 2f), t: d'2024-01-02T03:04:05Z', u: u'00000000-0000-0000-0000-000000000001', y: b"6162" }, { du: 0ns, pt: NONE, t: d'1000-01-01T00:00:00Z', u: u'00000000-0000-0000-0000-000000000003', y: b"" }]"""

[[test.results]]
value = "[{ count: 4 }]"

[[test.results]]
value = "[{ count: 0 }]"

[[test.results]]
value = "[{ c: 2, lit: 'blue', si: 9 }, { c: 3, lit: 'green', si: -9223372036854775794 }, { c: 3, lit: 'red', si: 12 }]"
*/

DEFINE TABLE kind SCHEMAFULL;
DEFINE FIELD i ON kind TYPE int;
DEFINE FIELD f ON kind TYPE float;
DEFINE FIELD d ON kind TYPE decimal;
DEFINE FIELD b ON kind TYPE bool;
DEFINE FIELD s ON kind TYPE string;
DEFINE FIELD t ON kind TYPE datetime;
DEFINE FIELD du ON kind TYPE duration;
DEFINE FIELD u ON kind TYPE uuid;
DEFINE FIELD y ON kind TYPE option<bytes>;
DEFINE FIELD o ON kind TYPE option<int>;
DEFINE FIELD n ON kind TYPE int | null;
DEFINE FIELD m ON kind TYPE option<int | null>;
DEFINE FIELD tags ON kind TYPE array<string>;
DEFINE FIELD link ON kind TYPE option<record<kind>>;
DEFINE FIELD lit ON kind TYPE 'red' | 'green' | 'blue';
DEFINE FIELD pt ON kind TYPE option<geometry<point>>;
CREATE kind:1 CONTENT { i: 1, f: 1.5, d: 2.25dec, b: true, s: 'one', t: d'2024-01-02T03:04:05Z', du: 90s, u: u'00000000-0000-0000-0000-000000000001', y: <bytes>'ab', o: 7, n: 3, m: 1, tags: ['x', 'y'], link: kind:2, lit: 'red', pt: (1.0, 2.0) };
CREATE kind:2 CONTENT { i: -9223372036854775807, f: -0.5, d: 0dec, b: false, s: '', t: d'9999-01-01T00:00:00Z', du: 600y, u: u'ffffffff-ffff-ffff-ffff-ffffffffffff', n: NULL, m: NULL, tags: [], lit: 'green' };
CREATE kind:3 CONTENT { i: 3, f: 3.0, d: 3dec, b: true, s: 'a string longer than twelve bytes', t: d'1000-01-01T00:00:00Z', du: 0s, u: u'00000000-0000-0000-0000-000000000003', y: <bytes>'', n: 4, tags: ['a'], lit: 'blue' };
CREATE kind:4 CONTENT { i: 4, f: 4.25, d: 4dec, b: false, s: 'four', t: d'2001-02-03T04:05:06Z', du: 1h, u: u'00000000-0000-0000-0000-000000000004', o: 11, n: NULL, m: 4, tags: ['q'], lit: 'red', pt: (-3.5, 0.0) };
CREATE kind:5 CONTENT { i: 5, f: 5.5, d: 5.5dec, b: true, s: 'five', t: d'2002-02-03T04:05:06.123456789Z', du: 2h30m, u: u'00000000-0000-0000-0000-000000000005', n: 5, tags: ['q', 'r'], lit: 'green' };
CREATE kind:6 CONTENT { i: 6, f: 6.0, d: 6dec, b: false, s: 'six', t: d'2003-02-03T04:05:06Z', du: 3h, u: u'00000000-0000-0000-0000-000000000006', o: 0, n: 6, m: NULL, tags: ['s'], lit: 'blue' };
CREATE kind:7 CONTENT { i: 7, f: 7.0, d: 7dec, b: true, s: 'seven', t: d'2004-02-03T04:05:06Z', du: 4h, u: u'00000000-0000-0000-0000-000000000007', o: 8, n: 7, tags: [], lit: 'red', link: kind:1 };
CREATE kind:8 CONTENT { i: 8, f: 8.0, d: 8dec, b: false, s: 'eight', t: d'2005-02-03T04:05:06Z', du: 5h, u: u'00000000-0000-0000-0000-000000000008', n: 8, tags: ['t', 'u'], lit: 'green', pt: (1.0, 2.0) };

SELECT * FROM kind;
SELECT count() FROM kind GROUP ALL;
SELECT math::sum(i) AS si, math::mean(f) AS mf, math::max(d) AS md FROM kind GROUP ALL;
SELECT VALUE id FROM kind ORDER BY i DESC;
SELECT VALUE id FROM kind ORDER BY s;
SELECT VALUE id FROM kind ORDER BY f DESC;
SELECT VALUE id FROM kind ORDER BY d;
SELECT VALUE id FROM kind ORDER BY t DESC;
SELECT VALUE id FROM kind ORDER BY du;
SELECT VALUE id FROM kind ORDER BY u DESC;
SELECT VALUE id FROM kind ORDER BY o;
SELECT VALUE id FROM kind ORDER BY n, id;
SELECT VALUE id FROM kind ORDER BY lit, id;
SELECT VALUE id FROM kind ORDER BY b, id;
SELECT VALUE id FROM kind ORDER BY i LIMIT 3;
SELECT * FROM kind WHERE i = 4;
SELECT * FROM kind WHERE i < 4;
SELECT VALUE id FROM kind WHERE i IN [1, 3, 5, 7];
SELECT VALUE id FROM kind WHERE i > 2 AND i < 7;
SELECT VALUE id FROM kind WHERE i = 4 OR b = true;
SELECT * FROM kind WHERE s = 'five';
SELECT VALUE id FROM kind WHERE s < 'f';
SELECT VALUE id FROM kind WHERE s IN ['one', 'six', 'none'];
SELECT VALUE id FROM kind WHERE s CONTAINS 'iv';
SELECT VALUE id FROM kind WHERE s = '';
SELECT VALUE id FROM kind WHERE f > 2.5;
SELECT VALUE id FROM kind WHERE f < 0;
SELECT VALUE id FROM kind WHERE f = 3;
SELECT VALUE id FROM kind WHERE d > 3dec;
SELECT VALUE id FROM kind WHERE d = 5.5dec;
SELECT VALUE id FROM kind WHERE b = true;
SELECT VALUE id FROM kind WHERE b = false AND i > 3;
SELECT VALUE id FROM kind WHERE t > d'2002-01-01T00:00:00Z';
SELECT VALUE id FROM kind WHERE t = d'2002-02-03T04:05:06.123456789Z';
SELECT VALUE id FROM kind WHERE t < d'1500-01-01T00:00:00Z';
SELECT VALUE id FROM kind WHERE t > d'3000-01-01T00:00:00Z';
SELECT VALUE id FROM kind WHERE du > 1h;
SELECT VALUE id FROM kind WHERE du = 90s;
SELECT VALUE id FROM kind WHERE du > 100y;
SELECT VALUE id FROM kind WHERE u = u'00000000-0000-0000-0000-000000000004';
SELECT VALUE id FROM kind WHERE u > u'00000000-0000-0000-0000-000000000005';
SELECT VALUE id FROM kind WHERE y = <bytes>'ab';
SELECT VALUE id FROM kind WHERE y IS NONE;
SELECT VALUE id FROM kind WHERE y IS NOT NONE;
SELECT VALUE id FROM kind WHERE o IS NONE;
SELECT VALUE id FROM kind WHERE o IS NOT NONE;
SELECT VALUE id FROM kind WHERE o = 0;
SELECT VALUE id FROM kind WHERE o > 7;
SELECT VALUE id FROM kind WHERE n IS NULL;
SELECT VALUE id FROM kind WHERE n = NULL;
SELECT VALUE id FROM kind WHERE n != NULL;
SELECT VALUE id FROM kind WHERE m IS NONE;
SELECT VALUE id FROM kind WHERE m IS NULL;
SELECT VALUE id FROM kind WHERE m = 1;
SELECT VALUE id FROM kind WHERE tags CONTAINS 'q';
SELECT VALUE id FROM kind WHERE tags = ['x', 'y'];
SELECT VALUE id FROM kind WHERE tags = [];
SELECT VALUE id FROM kind WHERE link = kind:2;
SELECT VALUE id FROM kind WHERE link IS NONE;
SELECT VALUE id FROM kind WHERE lit = 'red';
SELECT VALUE id FROM kind WHERE lit IN ['red', 'blue'];
SELECT VALUE id FROM kind WHERE lit != 'green';
SELECT VALUE id FROM kind WHERE pt IS NONE;
SELECT VALUE id FROM kind WHERE pt = (1.0, 2.0);
SELECT VALUE id FROM kind WHERE pt IS NOT NONE;
SELECT VALUE s FROM kind WHERE i = 4;
SELECT VALUE i FROM kind WHERE b = true;
SELECT i, s, b FROM kind WHERE i > 5;
SELECT type::is_int(i) AS int_i, type::is_float(f) AS float_f, type::is_decimal(d) AS dec_d FROM kind WHERE i = 5;
SELECT VALUE [type::is_none(o), type::is_null(n), type::is_none(m), type::is_null(m)] FROM kind WHERE i IN [2, 4, 6, 8];
SELECT s, i FROM kind WHERE i = 3;
SELECT t, du, u, y, pt FROM kind WHERE i IN [1, 2, 3];
SELECT count() FROM kind WHERE b = true GROUP ALL;
SELECT count() FROM kind WHERE i = 99 GROUP ALL;
SELECT lit, count() AS c, math::sum(i) AS si FROM kind GROUP BY lit ORDER BY lit;
