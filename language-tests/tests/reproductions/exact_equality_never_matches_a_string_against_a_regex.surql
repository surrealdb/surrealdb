/**
[env]
namespace = true
database = true

[test]
requires = ["regex"]
reason = "`==` compares a regex with a string as two different kinds of value, so a string is never exactly equal to a regex, whatever the regex matches; `=` matches the string against the regex instead. A scan filter negating `v == /^a/` therefore keeps a row holding the string 'abc' — on its own, reversed, and fused with a second comparison into one scan of the row. The closure in each projection keeps the scan body out of the storage cursor, which makes the scan judge each record's stored bytes with the pre-decode filter before decoding it."

[[test.results]]
value = "[{ id: r:1, n: 1, tags: ['x'], v: 'abc' }]"

[[test.results]]
value = "[{ id: r:2, n: 1, tags: ['x'], v: 'xyz' }]"

[[test.results]]
value = "[{ id: r:3, n: 1, tags: ['x'], v: /^a/ }]"

[[test.results]]
value = "[{ id: r:4, n: 1, tags: ['x'], v: /^b/ }]"

[[test.results]]
value = "[false, true, true, false]"

[[test.results]]
value = "[{ id: r:1, seen: ['x'] }, { id: r:2, seen: ['x'] }, { id: r:4, seen: ['x'] }]"

[[test.results]]
value = "[{ id: r:1, seen: ['x'] }, { id: r:2, seen: ['x'] }, { id: r:4, seen: ['x'] }]"

[[test.results]]
value = "[{ id: r:1, seen: ['x'] }, { id: r:2, seen: ['x'] }, { id: r:4, seen: ['x'] }]"

[[test.results]]
value = "[{ id: r:3, seen: ['x'] }]"

[[test.results]]
value = "[{ id: r:2, seen: ['x'] }, { id: r:4, seen: ['x'] }]"
*/

CREATE r:1 SET v = 'abc', n = 1, tags = ['x'];
CREATE r:2 SET v = 'xyz', n = 1, tags = ['x'];
CREATE r:3 SET v = /^a/, n = 1, tags = ['x'];
CREATE r:4 SET v = /^b/, n = 1, tags = ['x'];

RETURN ['abc' == /^a/, 'abc' = /^a/, /^a/ == /^a/, /^a/ == /^b/];

SELECT id, tags.map(|$t| $t) AS seen FROM r WHERE !(v == /^a/);
SELECT id, tags.map(|$t| $t) AS seen FROM r WHERE !(/^a/ == v);
SELECT id, tags.map(|$t| $t) AS seen FROM r WHERE !(v == /^a/ AND n = 1);
SELECT id, tags.map(|$t| $t) AS seen FROM r WHERE v == /^a/;
SELECT id, tags.map(|$t| $t) AS seen FROM r WHERE !(v = /^a/);
