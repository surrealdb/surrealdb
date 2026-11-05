/**
[env]
namespace = true
database = true

[test]
reason = "A statement that a method argument's closure holds, on a receiver read from a name (a field path of a `LET` value), reads `$this` as the document of the frame that invokes the closure, which a frame with no row does not have, so the statement reads no rows whether or not the body also reads `$this` elsewhere. A block in the body takes the document of the frame that invokes the closure as its `$parent`, which a frame with no row does not have, so it reads `NONE`. The closure-free twin (`concat`) reads the receiver as the step's root."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[], []]"

[[test.results]]
value = "[[], []]"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "[{ lim: 1 }, { lim: 2 }, [1, 2]]"
*/

LET $o = { a: [{ lim: 1 }, { lim: 2 }] };
RETURN $o.a.map(|$e| (SELECT VALUE lim FROM $this));
RETURN $o.a.map(|$e| [$this, (SELECT VALUE lim FROM $this)][1]);
RETURN $o.a.map(|$e| { LET $z = $parent; $z });
RETURN $o.a.concat([(SELECT VALUE lim FROM $this)]);
