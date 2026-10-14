/**
[env]
namespace = true
database = true

[test]
reason = "A closure written in a frame that binds `$this` or `$self` reads that binding, whether the literal is a method argument, is invoked inside an argument, is nested in the body of another such closure, or is written in a subquery of another closure's body. `LET $this` and `LET $self` are refused wherever they are written, in a block, in a clause and at the top level, so each statement that holds one raises the refusal. A `$this` read where none is bound is the current value of the frame: none at the top level, the row in a statement over rows, and the row a subquery's source yields inside a subquery."

[[test.results]]
value = "[{ id: ts:1, lim: 1 }]"

[[test.results]]
value = "[{ id: ts:2, lim: 8 }]"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'self' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'self' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[[1], [8]]"

[[test.results]]
value = "[1, 8]"
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
