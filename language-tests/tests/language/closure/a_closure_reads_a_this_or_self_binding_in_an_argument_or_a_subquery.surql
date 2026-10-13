/**
[env]
namespace = true
database = true

[test]
reason = "A closure written where a binding holds `$this` or `$self` (a block `LET`, a statement `LET`) reads that binding, whether the literal is a method argument, is invoked inside an argument, is nested in the body of another such closure, or is written in a subquery of another closure's body, and whether or not that body reads the name anywhere else. The binding is not a constant in the second statement, which a closure-free read would take as the receiver. A name no binding holds is the root of the argument. In a frame with a row, a statement `LET` is read by a literal invoked in place and not by a method argument's closure, whose `$this` is the row."

[[test.results]]
value = "[{ id: ts:1, lim: 1 }]"

[[test.results]]
value = "[{ id: ts:2, lim: 8 }]"

[[test.results]]
value = "[8]"

[[test.results]]
value = "[8]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[1, 3]"

[[test.results]]
value = "[[3], [3]]"

[[test.results]]
value = "[[3]]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3, [3]]"

[[test.results]]
value = "[NONE, [3]]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[[3], [3]]"

[[test.results]]
value = "[[3], [3]]"

[[test.results]]
value = "[[3, 1], [3, 8]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[[1], [8]]"

[[test.results]]
value = "[3, 3]"

*/

CREATE ts:1 SET lim = 1;
CREATE ts:2 SET lim = 8;
RETURN { LET $this = 7; [1].map(|$e| $e + $this) };
RETURN { LET $this = <int> "7"; [1].map(|$e| $e + $this) };
RETURN { LET $this = { lim: 3 }; [1].map(|$e| $this.lim) };
RETURN { LET $this = { lim: 3 }; [{ lim: 9 }].map(|$e| $this.lim) };
RETURN { LET $self = { lim: 3 }; [{ lim: 9 }].map(|$e| $self.lim) };
RETURN { LET $this = { lim: 3 }; [1, 5].filter(|$e| $e > $this.lim) };
RETURN { LET $this = { lim: 3 }; array::map([{ lim: 9 }], |$e| $this.lim) };
RETURN { LET $this = { lim: 3 }; [1].concat([(|$e| $this.lim)(0)]) };
RETURN { LET $this = { lim: 3 }; [1, 2].map(|$e| [3].map(|$f| $this.lim)) };
RETURN { LET $this = { lim: 3 }; [{ lim: 9 }].map(|$e| [{ lim: 7 }].map(|$f| $this.lim)) };
RETURN { LET $this = { lim: 3 }; (|$e| (SELECT VALUE (|$f| $this.lim)(0) FROM [{ lim: 5 }]))(0) };
RETURN { LET $this = { lim: 3 }; (|$e| [$this.lim, (SELECT VALUE (|$f| $this.lim)(0) FROM [{ lim: 5 }])])(0) };
RETURN { LET $this = { lim: 3 }; (|$e| [$self, (SELECT VALUE (|$f| $this.lim)(0) FROM [{ lim: 5 }])])(0) };
RETURN { LET $this = { lim: 3 }; (|$e| { (SELECT VALUE (|$f| $this.lim)(0) FROM [{ lim: 5 }]) })(0) };
RETURN { LET $self = { lim: 3 }; (|$e| (SELECT VALUE (|$f| $self.lim)(0) FROM [{ lim: 5 }]))(0) };
RETURN { LET $this = { lim: 3 }; (SELECT VALUE (|$f| $this.lim)(0) FROM [{ lim: 5 }]) };
SELECT VALUE { LET $this = { lim: 3 }; [1].map(|$e| $this.lim) } FROM ts ORDER BY id;
UPDATE ts SET z = { LET $this = { lim: 3 }; [1].map(|$e| $this.lim) } RETURN VALUE z;
SELECT VALUE { LET $this = { lim: 3 }; (|$e| [$this.lim, lim])(0) } FROM ts ORDER BY id;
LET $this = { lim: 3 };
RETURN [1].map(|$e| $this.lim);
RETURN [{ lim: 9 }].map(|$e| $this.lim);
RETURN (|$e| (SELECT VALUE (|$f| $this.lim)(0) FROM [{ lim: 5 }]))(0);
RETURN (SELECT VALUE (|$f| $this.lim)(0) FROM [{ lim: 5 }]);
SELECT VALUE [1].map(|$e| $this.lim) FROM ts ORDER BY id;
SELECT VALUE (|$e| $this.lim)(0) FROM ts ORDER BY id;
