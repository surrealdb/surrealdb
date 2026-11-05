/**
[env]

[test]

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = "[{ id: t:2, links: [a:3, a:4] }]"

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

[[test.results]]
value = "[{ id: t:1, links: [a:2, a:1] }]"

*/
CREATE t:1 SET links = [a:2, a:1];
CREATE t:2 SET links = [a:3, a:4];

SELECT * FROM t WHERE links CONTAINSANY [a:2];
SELECT * FROM t WHERE links CONTAINS a:2;
SELECT * FROM t WHERE a:2 IN links;
SELECT * FROM t WHERE [a:2] ANYINSIDE links;

DEFINE INDEX idx ON t FIELDS links;

SELECT * FROM t WHERE links CONTAINSANY [a:2];
SELECT * FROM t WHERE links CONTAINS a:2;
SELECT * FROM t WHERE a:2 IN links;
SELECT * FROM t WHERE [a:2] ANYINSIDE links;
