/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "`id = <record path>` and `id IN <record path>` (also through `type::field('id')`, and inside a composite key), where the path reads a record whose field holds the record ids, read those records by key and return the rows a scan filtered by the same predicate returns."

[[test.results]]
value = "[{ first: rf:3, id: holder:1, targets: [rf:2, rf:1, other:1] }]"

[[test.results]]
value = "[{ id: rf:1, v: 10 }]"

[[test.results]]
value = "[{ id: rf:2, v: 20 }]"

[[test.results]]
value = "[{ id: rf:3, v: 30 }]"

[[test.results]]
value = "[rf:1, rf:2]"

[[test.results]]
value = "[rf:2]"

[[test.results]]
value = "[rf:3]"

[[test.results]]
value = "[rf:1, rf:2]"

[[test.results]]
value = "[rf:3]"

[[test.results]]
value = "[{ id: pair:[1, 2], v: 'p' }]"

[[test.results]]
value = "[{ id: holder:2, n: 1 }]"

[[test.results]]
value = "[pair:[1, 2]]"
*/

CREATE holder:1 SET targets = [rf:2, rf:1, other:1], first = rf:3;
CREATE rf:1 SET v = 10;
CREATE rf:2 SET v = 20;
CREATE rf:3 SET v = 30;
SELECT VALUE id FROM rf WHERE id IN holder:1.targets;
SELECT VALUE id FROM rf WHERE id = holder:1.targets[0];
SELECT VALUE id FROM rf WHERE id = holder:1.first;
SELECT VALUE id FROM rf WITH NOINDEX WHERE id IN holder:1.targets OR false;
SELECT VALUE id FROM rf WHERE type::field('id') = holder:1.first;
CREATE pair:[1, 2] SET v = 'p';
CREATE holder:2 SET n = 1;
SELECT VALUE id FROM pair WHERE id = pair:[holder:2.n, 2];
