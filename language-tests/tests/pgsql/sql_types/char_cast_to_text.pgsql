/**
[env]
namespace = true
database = true

[test]
reason = "a cast of a char(n), character or bpchar value to text or varchar drops the trailing spaces that pad it, for a literal, a char column and a varchar cast through char, while a varchar keeps its trailing spaces"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ c: 'ab', id: 1, v: 'ab  ' }]"

[[test.results]]
value = "[{ b: 'ab', c: 'ab', e: 'ab', g: 'ab', n: 1, t: 'a', w: 'ab' }]"

[[test.results]]
value = "[{ c: 'ab', cn: 2, v: 'ab  ', vn: 4 }]"

[[test.results]]
value = "[{ c: 'ab|', g: 'ab', vc: 'ab' }]"
*/

CREATE TABLE pad (id int PRIMARY KEY, c char(4), v varchar(4));

INSERT INTO pad (id, c, v) VALUES (1, 'ab', 'ab  ');

SELECT 'a'::char(3)::text AS t, length('a'::char(3)::text) AS n, 'ab  '::bpchar::text AS b, 'ab  '::char(4)::text AS c, 'ab  '::character(4)::text AS e, CAST(CAST('ab  ' AS bpchar) AS text) AS g, 'ab  '::char(4)::varchar AS w;

SELECT c::text AS c, length(c::text) AS cn, v::text AS v, length(v::text) AS vn FROM pad;

SELECT c::text || '|' AS c, v::char(4)::text AS vc, CAST(CAST(v AS bpchar) AS text) AS g FROM pad;
