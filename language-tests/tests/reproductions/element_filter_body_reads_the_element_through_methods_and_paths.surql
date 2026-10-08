/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A [WHERE ...] element filter whose body calls methods, indexes, nests a filter, maps with a closure, builds an object or destructures reads each element as its current value"

[[test.results]]
value = "[{ id: p:1, items: [{ n: 1, tags: ['a'] }, { n: 2, tags: [] }, { n: 3, tags: ['b', 'c'] }] }]"

[[test.results]]
value = "[{ id: p:2, items: [{ k: 'tags', n: 4, tags: ['x'] }] }]"

[[test.results]]
value = "[[1, 3], [4]]"

[[test.results]]
value = "[[3], []]"

[[test.results]]
value = "[[3], []]"

[[test.results]]
value = "[[3], []]"

[[test.results]]
value = "[[2], []]"

[[test.results]]
value = "[[1], [4]]"

[[test.results]]
value = "[[3], []]"

[[test.results]]
value = "[[1, 2]]"

[[test.results]]
value = "[[3]]"

[[test.results]]
value = "[[], [4]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: q:1, v: [] }]"

[[test.results]]
value = "[{ id: q:2, v: [[1]] }]"
*/
CREATE p:1 SET items = [{ n: 1, tags: ['a'] }, { n: 2, tags: [] }, { n: 3, tags: ['b', 'c'] }];
CREATE p:2 SET items = [{ n: 4, tags: ['x'], k: 'tags' }];
SELECT VALUE items[WHERE tags.len() > 0].n FROM p ORDER BY id;
SELECT VALUE items[WHERE tags[0] = 'b'].n FROM p ORDER BY id;
SELECT VALUE items[WHERE tags[WHERE $this = 'c'].len() > 0].n FROM p ORDER BY id;
SELECT VALUE items[WHERE tags.map(|$t| $t + '!').len() > 1].n FROM p ORDER BY id;
SELECT VALUE items[WHERE $this.tags.len() = 0].n FROM p ORDER BY id;
SELECT VALUE items[WHERE {a: tags}.a.len() = 1].n FROM p ORDER BY id;
SELECT VALUE items[WHERE $this.{tags}.tags.len() = 2].n FROM p ORDER BY id;
RETURN [[1,2],[3]][WHERE $this.len() = 2];
RETURN [[1,2],[3]][WHERE $this[0] = 3];
SELECT VALUE items[WHERE n > $parent.items.len()].n FROM p ORDER BY id;
-- A field assertion holding an element filter is judged on every write.
DEFINE FIELD v ON q ASSERT $value[WHERE $this.len() = 0] = [];
CREATE q:1 SET v = [];
CREATE q:2 SET v = [[1]];
