/**
[env]
namespace = true
database = true

[test]
reason = "PostgreSQL ltrim and rtrim take their character set from any text expression, such as a column; an empty set removes nothing, a string made only of the set trims to the empty string, and a NULL string or set answers NULL"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ chars: 'xy', id: 1, s: 'xxabcyy' }, { chars: '', id: 2, s: 'abc' }, { chars: 'xy', id: 3, s: 'xyx' }, { chars: 'x', id: 4, s: NULL }, { chars: NULL, id: 5, s: 'xa' }, { chars: '→', id: 6, s: '→→a→' }, { chars: '-[]', id: 7, s: '[a-]' }]"

[[test.results]]
value = "[{ id: 1, l: 'abcyy', r: 'xxabc' }, { id: 2, l: 'abc', r: 'abc' }, { id: 3, l: '', r: '' }, { id: 4, l: NULL, r: NULL }, { id: 5, l: NULL, r: NULL }, { id: 6, l: 'a→', r: '→→a' }, { id: 7, l: 'a-]', r: '[a' }]"

[[test.results]]
value = "[{ a: 'abc', b: 'abc', c: 'a', d: '[a' }]"

[[test.results]]
value = "[{ a: 'abc' }]"
*/

CREATE TABLE trims (id INT, s TEXT, chars TEXT);

INSERT INTO trims (id, s, chars) VALUES (1, 'xxabcyy', 'xy'), (2, 'abc', ''), (3, 'xyx', 'xy'), (4, NULL, 'x'), (5, 'xa', NULL), (6, '→→a→', '→'), (7, '[a-]', '-[]');

SELECT id, ltrim(s, chars) AS l, rtrim(s, chars) AS r FROM trims ORDER BY id;

SELECT ltrim('abc', '') AS a, rtrim('abc', '') AS b, ltrim('→→a', '→') AS c, rtrim('[a-]', '-[]') AS d;

SELECT ltrim('xxabc', chars) AS a FROM trims WHERE id = 1;
