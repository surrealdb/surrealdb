/**
[env]
namespace = true
database = true

[test]
reason = "A destructure alias reads the value destructured as its document: as `$this`, `$self` and `$parent`, with or without a block around it and in a statement the alias holds, and the bare fields of the alias read it too. The row of the frame around the destructure is not read, so a destructure in a SELECT projection, a SET value or a closure answers as one in a RETURN does. An alias of a nested pattern reads the value the outer pattern destructures. A statement in an alias that reads a source of its own reads that source's row as `$this`."

[[test.results]]
value = "[{ id: w:1, v: 2 }]"

[[test.results]]
value = "[{ b: 9 }]"

[[test.results]]
value = "[{ b: 9 }]"

[[test.results]]
value = "[{ a: 9, b: 9, c: 9, d: 9 }]"

[[test.results]]
value = "[{ b: [9, 9, 9] }]"

[[test.results]]
value = "[{ c: 9 }]"

[[test.results]]
value = "[[{ b: 9 }]]"

[[test.results]]
value = "[{ v: { k: [{ k: 9 }, { k: 9 }, { k: 9 }] } }]"

[[test.results]]
value = "[{ v: { k: [NONE, NONE, NONE] } }]"

[[test.results]]
value = "[{ b: 4 }]"

[[test.results]]
value = "{ b: 9, c: 9, d: 9 }"

[[test.results]]
value = "{ a: 1 }"
*/

CREATE w:1 SET v = 2;
SELECT VALUE { v: 9 }.{ b: $this.v } FROM w;
SELECT VALUE { v: 9 }.{ b: $self.v } FROM w;
SELECT VALUE { v: 9 }.{ a: { $parent.v }, b: { $this.v }, c: { v }, d: { $self.v } } FROM w;
SELECT VALUE { v: 9 }.{ b: { LET $q = 1; [$this.v, $self.v, $parent.v] } } FROM w;
UPDATE w:1 SET z = { v: 9 }.{ c: { $this.v } } RETURN VALUE z;
SELECT VALUE [{ v: 9 }].map(|$e| $e.{ b: { $this.v } }) FROM w;
SELECT VALUE { v: { k: 9 } }.{ v: { k: [$this.v, $parent.v, $self.v] } } FROM w;
SELECT VALUE { v: { k: 9 } }.{ v: { k: { [$this.k, $parent.k, k] } } } FROM w;
SELECT VALUE { v: 9 }.{ b: (SELECT VALUE $this.v FROM ONLY { v: 4 }) } FROM w;
RETURN { v: 9 }.{ b: $this.v, c: { $self.v }, d: { $this.v } };
RETURN { a: 1 }.{ a: { LET $x = 1; $x } };
