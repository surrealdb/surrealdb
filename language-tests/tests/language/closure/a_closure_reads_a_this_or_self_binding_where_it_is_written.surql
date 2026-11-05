/**
[env]
namespace = true
database = true

[test]
reason = "A closure whose body reads `$this` or `$self` reads the binding of that name that holds where the literal is written: a `FOR` variable or a function parameter. The closure-free read at the same place is the control, and an unbound name is the current value, as it is without a binding. `LET $this` and `LET $self` are refused where they are written inside a frame — in a block, in a clause, in a field definition — so each statement that holds one raises the refusal and a `$this` or `$self` read after a refused `LET` is unbound. A `LET` that is a whole statement binds a request variable instead, which a closure written outside a row reads and a row-bearing statement shadows with its row."

[[test.results]]
value = "[{ id: pt:1, lim: 1 }]"

[[test.results]]
value = "[{ id: pt:2, lim: 8 }]"

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3]"

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
value = "[[NONE, NONE], [NONE, NONE]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 8]"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
error = "'this' is a protected variable and cannot be set"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "[3, 3]"

[[test.results]]
value = "[1, 8]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"
*/
CREATE pt:1 SET lim = 1;
CREATE pt:2 SET lim = 8;
RETURN { LET $this = { lim: 3 }; (|$e| $this.lim)(0) };
RETURN { LET $this = { lim: 3 }; $this.lim };
RETURN { LET $self = { lim: 3 }; (|$e| $self.lim)(0) };
RETURN { LET $this = { lim: 3 }; LET $f = |$e| $this.lim; $f(0) };
RETURN { LET $this = 5; (|$e| $e < $this)(1) };
RETURN { LET $this = 5; 1 < $this };
RETURN { LET $this = { lim: 3 }; (|$e| { (|$f| $this.lim)(0) })(0) };
RETURN { LET $this = { lim: 3 }; (|$e| { $this.lim + (|$f| $this.lim)(0) })(0) };
DEFINE FUNCTION fn::tp($this: any) { RETURN (|$e| $this.lim)(0); };
DEFINE FUNCTION fn::tp2($this: any) { RETURN $this.lim; };
RETURN fn::tp({ lim: 3 });
RETURN fn::tp2({ lim: 3 });
DEFINE FUNCTION fn::ts($this: any) { LET $f = |$e| $e + $this.lim; RETURN $f(1); };
RETURN fn::ts({ lim: 3 });
FOR $this IN [{ lim: 3 }] { LET $v = (|$e| $this.lim)(0); CREATE log SET v = $v; };
FOR $this IN [{ lim: 3 }] { LET $v = $this.lim; CREATE log2 SET v = $v; };
SELECT VALUE v FROM log;
SELECT VALUE v FROM log2;
SELECT VALUE { LET $this = { lim: 3 }; (|$e| $this.lim)(0) } FROM pt ORDER BY id;
SELECT VALUE { LET $this = { lim: 3 }; $this.lim } FROM pt ORDER BY id;
SELECT VALUE { LET $this = { lim: 3 }; (|$e| [lim, $this.lim])(0) } FROM pt ORDER BY id;
UPDATE pt SET z = { LET $this = { lim: 3 }; (|$e| $this.lim)(0) } RETURN VALUE z;
UPDATE pt SET z = { LET $this = { lim: 3 }; $this.lim } RETURN VALUE z;
DEFINE FIELD y ON pt VALUE { LET $this = { lim: 3 }; (|$e| $this.lim)(0) };
DEFINE FIELD y2 ON pt VALUE { LET $this = { lim: 3 }; $this.lim };
UPDATE pt RETURN VALUE [y, y2];
RETURN (|$e| $this)(0);
SELECT VALUE (|$e| $this.lim)(0) FROM pt ORDER BY id;
RETURN { LET $this = 7; [1].map(|$e| $e + $this) };
RETURN { LET $this = math::abs(time::unix(time::now()) * 0 + 7); [1].concat([1 + $this]) };
LET $this = { lim: 3 };
RETURN (|$e| $this.lim)(0);
RETURN $this.lim;
LET $f = |$e| $this.lim;
RETURN $f(0);
SELECT VALUE $f(0) FROM pt ORDER BY id;
SELECT VALUE (|$e| $this.lim)(0) FROM pt ORDER BY id;
LET $self = { lim: 3 };
RETURN (|$e| $self.lim)(0);
RETURN (|$e| { (|$f| $this.lim)(0) })(0);
