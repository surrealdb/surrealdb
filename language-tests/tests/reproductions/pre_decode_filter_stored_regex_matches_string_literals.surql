/**
[env]
namespace = true
database = true

[test]
requires = ["regex"]
reason = "A stored regex equals every string it matches, so a scan filter comparing it with string literals keeps the row: through IN, a fused `=` chain, CONTAINSANY and CONTAINSALL, and through a negated NOT IN or CONTAINSNONE. The closure in each projection keeps the scan body out of the storage cursor, which makes the scan judge each record's stored bytes with the pre-decode filter before decoding it."

[[test.results]]
value = "[{ id: r:1, tags: [/^a/], v: /^a/ }]"

[[test.results]]
value = "[{ id: r:2, tags: [/^z/], v: /^z/ }]"

[[test.results]]
value = "true"

[[test.results]]
value = "[{ id: r:1, seen: [/^a/] }]"

[[test.results]]
value = "[{ id: r:1, seen: [/^a/] }]"

[[test.results]]
value = "[{ id: r:1, seen: [/^a/] }]"

[[test.results]]
value = "[{ id: r:1, seen: [/^a/] }]"

[[test.results]]
value = "[{ id: r:1, seen: [/^a/] }]"

[[test.results]]
value = "[{ id: r:1, seen: [/^a/] }]"
*/

CREATE r:1 SET v = /^a/, tags = [/^a/];
CREATE r:2 SET v = /^z/, tags = [/^z/];

RETURN [/^a/] CONTAINSANY ['abc'];

SELECT id, tags.map(|$t| $t) AS seen FROM r WHERE v IN ['abc'];
SELECT id, tags.map(|$t| $t) AS seen FROM r WHERE v = 'abc' OR v = 'xyz';
SELECT id, tags.map(|$t| $t) AS seen FROM r WHERE tags CONTAINSANY ['abc'];
SELECT id, tags.map(|$t| $t) AS seen FROM r WHERE tags CONTAINSALL ['abc', 'axe'];
SELECT id, tags.map(|$t| $t) AS seen FROM r WHERE !(v NOT IN ['abc']);
SELECT id, tags.map(|$t| $t) AS seen FROM r WHERE !(tags CONTAINSNONE ['abc']);
