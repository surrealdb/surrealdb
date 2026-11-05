/**
[env]
timeout = 5000

[test]
reason = "A COMPUTED object field (the successor of <future>) returns the property when a path is selected, and ORDER BY on the path agrees with SELECT *"
issue = 6582

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: borked:1, name: 'a', usages: { count: 1, lastUsed: d'2025-01-02T00:00:00Z' } }]"

[[test.results]]
value = "[{ id: borked:2, name: 'bbb', usages: { count: 3, lastUsed: d'2025-01-04T00:00:00Z' } }]"

[[test.results]]
value = "[{ id: borked:3, name: 'cc', usages: { count: 2, lastUsed: d'2025-01-03T00:00:00Z' } }]"

[[test.results]]
value = "['bbb', 'cc', 'a']"

[[test.results]]
value = "['bbb', 'cc', 'a']"

[[test.results]]
value = "[{ name: 'a', usages: { lastUsed: d'2025-01-02T00:00:00Z' } }, { name: 'bbb', usages: { lastUsed: d'2025-01-04T00:00:00Z' } }, { name: 'cc', usages: { lastUsed: d'2025-01-03T00:00:00Z' } }]"
*/

DEFINE TABLE borked;
DEFINE FIELD name ON borked TYPE string;
DEFINE FIELD usages ON borked COMPUTED { count: <int>string::len(name), lastUsed: d'2025-01-01' + 1d * <int>string::len(name) };
CREATE borked:1 SET name = 'a';
CREATE borked:2 SET name = 'bbb';
CREATE borked:3 SET name = 'cc';
SELECT VALUE name FROM (SELECT * FROM borked ORDER usages.lastUsed DESC);
SELECT VALUE name FROM (SELECT name, usages.lastUsed FROM borked ORDER usages.lastUsed DESC);
SELECT name, usages.lastUsed FROM borked ORDER BY name;
