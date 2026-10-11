/**
[env]
namespace = true
database = true

[test]
reason = "`==` compares a regex with a string as two different kinds of value, so a string is never exactly equal to a regex, whatever the regex matches; `=` matches the string against the regex instead. A scan filter negating `v == /^a/` therefore keeps a row holding the string 'abc' — on its own, reversed, and fused with a second comparison into one scan of the row. Each table scan judges a record's stored bytes with the pre-decode filter before decoding it."

[[test.results]]
value = "[{ id: r:1, n: 1, v: 'abc' }]"

[[test.results]]
value = "[{ id: r:2, n: 1, v: 'xyz' }]"

[[test.results]]
value = "[{ id: r:3, n: 1, v: /^a/ }]"

[[test.results]]
value = "[{ id: r:4, n: 1, v: /^b/ }]"

[[test.results]]
value = "[false, true, true, false]"

[[test.results]]
value = "[{ id: r:1 }, { id: r:2 }, { id: r:4 }]"

[[test.results]]
value = "[{ id: r:1 }, { id: r:2 }, { id: r:4 }]"

[[test.results]]
value = "[{ id: r:1 }, { id: r:2 }, { id: r:4 }]"

[[test.results]]
value = "[{ id: r:3 }]"

[[test.results]]
value = "[{ id: r:2 }, { id: r:4 }]"
*/

CREATE r:1 SET v = 'abc', n = 1;
CREATE r:2 SET v = 'xyz', n = 1;
CREATE r:3 SET v = /^a/, n = 1;
CREATE r:4 SET v = /^b/, n = 1;

RETURN ['abc' == /^a/, 'abc' = /^a/, /^a/ == /^a/, /^a/ == /^b/];

SELECT id FROM r WHERE !(v == /^a/);
SELECT id FROM r WHERE !(/^a/ == v);
SELECT id FROM r WHERE !(v == /^a/ AND n = 1);
SELECT id FROM r WHERE v == /^a/;
SELECT id FROM r WHERE !(v = /^a/);
