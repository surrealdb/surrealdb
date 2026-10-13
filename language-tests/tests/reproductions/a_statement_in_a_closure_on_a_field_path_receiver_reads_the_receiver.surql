/**
[env]
namespace = true
database = true

[test]
reason = "A statement or a block that a method argument's closure holds, on a receiver read from a name (a field path of a `LET` value), reads the current value of the step through `$this` and `$parent`, which in a frame with no row is the method's receiver, whether or not the body also reads the current value elsewhere, as the closure-free twin does."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1, 2], [1, 2]]"

[[test.results]]
value = "[[1, 2], [1, 2]]"

[[test.results]]
value = "[[{ lim: 1 }, { lim: 2 }], [{ lim: 1 }, { lim: 2 }]]"

[[test.results]]
value = "[{ lim: 1 }, { lim: 2 }, [1, 2]]"
*/

LET $o = { a: [{ lim: 1 }, { lim: 2 }] };
RETURN $o.a.map(|$e| (SELECT VALUE lim FROM $this));
RETURN $o.a.map(|$e| [$this, (SELECT VALUE lim FROM $this)][1]);
RETURN $o.a.map(|$e| { LET $z = $parent; $z });
RETURN $o.a.concat([(SELECT VALUE lim FROM $this)]);
