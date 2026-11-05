/**
[env]
namespace = true
database = true

[test]
reason = "Inside an element filter, `$this` in a block reads the element, the same as a bare field: braces never change what `$this` means. The row the filter sits in stays reachable as `$parent`. Owner ruling 4b (2026-10-14)."

[[test.results]]
value = "[{ id: post:1, items: [{ v: 1 }, { v: 5 }] }]"

[[test.results]]
value = "[{ id: post:2, items: [{ v: 1 }, { v: 5 }], min: 3 }]"

[[test.results]]
value = "[[{ v: 5 }]]"

[[test.results]]
value = "[[{ v: 5 }]]"

[[test.results]]
value = "[[{ v: 5 }]]"
*/

CREATE post:1 SET items = [{ v: 1 }, { v: 5 }];
CREATE post:2 SET min = 3, items = [{ v: 1 }, { v: 5 }];
SELECT VALUE items[WHERE { $this.v > 1 }] FROM post:1;
SELECT VALUE items[WHERE { v > 1 }] FROM post:1;
SELECT VALUE items[WHERE { $this.v > $parent.min }] FROM post:2;
