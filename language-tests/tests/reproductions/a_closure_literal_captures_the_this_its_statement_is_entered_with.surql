/**
[env]
namespace = true
database = true

[test]
reason = "A closure literal written in a statement that is entered from a frame with a row captures the `$this` that statement is entered with, the document of the frame around the statement, as it captures a variable: invoked where it is written, or inside a closure invoked there, its body reads that document as `$this`, not the row the statement reads. `$self`, the fields and `@` of the body still read the row the closure is invoked over, `$parent` reads the document as it does in the statement, and a block in the body binds `$this` to that row again. A literal that an array builtin runs in place over its elements (`map`, `filter`, `find`, `any`, `all` and their aliases) reads `$this` as the call does, and hands the captured `$this` on to a literal its body invokes. A statement entered from a frame with no row binds no `$this`, so a literal there, like one assigned by `LET` at the top level, reads the row it is invoked over."

[[test.results]]
value = "[{ id: qv:one, v: 1 }]"

[[test.results]]
value = "[{ id: w:1, v: 2 }]"

[[test.results]]
value = "[[[5, 2]]]"

[[test.results]]
value = "[[[5, 2, 1, 2]]]"

[[test.results]]
value = "[[{ id: w:1, v: 2 }]]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[[[2]]]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "[[1]]"

[[test.results]]
value = "[[2]]"

[[test.results]]
value = "[[2]]"

[[test.results]]
value = "[[[1]]]"

[[test.results]]
value = "[[[0]]]"

[[test.results]]
value = "[[[2]]]"

[[test.results]]
value = "[[[2]]]"

[[test.results]]
value = "[[2]]"

[[test.results]]
value = "[[2]]"

[[test.results]]
value = "[[NONE]]"

[[test.results]]
value = "[[5, 1]]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1]]"
*/

CREATE qv:one SET v = 1;
CREATE w:1 SET v = 2;
SELECT VALUE (SELECT VALUE (|$self| [$self, $this.v])(5) FROM qv) FROM w;
SELECT VALUE (SELECT VALUE (|$x| [$x, $this.v, v, $parent.v])(5) FROM qv) FROM w;
SELECT VALUE (SELECT VALUE (|$x| $this)(5) FROM qv) FROM w;
SELECT VALUE (SELECT VALUE (|$x| $self.v)(5) FROM qv) FROM w;
SELECT VALUE (SELECT VALUE (|$x| { $this.v })(5) FROM qv) FROM w;
SELECT VALUE (SELECT VALUE (SELECT VALUE (|$x| $this.v)(5) FROM qv) FROM w) FROM qv;
SELECT VALUE (SELECT VALUE (|$x| $this.v)(5) FROM ONLY { v: 7 }) FROM w;
SELECT VALUE (SELECT VALUE v FROM qv WHERE (|$x| $this.v)(0) = 2) FROM w;
SELECT VALUE (SELECT VALUE (|$x| (|$y| $this.v)(0))(5) FROM qv) FROM w;
SELECT VALUE (SELECT VALUE [0].fold(0, |$a, $x| $this.v) FROM qv) FROM w;
SELECT VALUE (SELECT VALUE [0].map(|$x| $this.v) FROM qv) FROM w;
SELECT VALUE (SELECT VALUE array::filter([0], |$x| $this.v = 1) FROM qv) FROM w;
SELECT VALUE (SELECT VALUE [0].map(|$x| (|$y| $this.v)(0)) FROM qv) FROM w;
SELECT VALUE (SELECT VALUE (|$x| [0].map(|$y| $this.v))(5) FROM qv) FROM w;
SELECT VALUE { LET $q = 1; RETURN (SELECT VALUE (|$x| $this.v)(5) FROM qv) } FROM w;
UPDATE w:1 SET z = (SELECT VALUE (|$x| $this.v)(5) FROM qv) RETURN VALUE z;
CREATE x:1 SET a = (SELECT VALUE (|$x| $this.v)(5) FROM qv) RETURN VALUE a;
SELECT VALUE (|$self| [$self, $this.v])(5) FROM qv;
RETURN (SELECT VALUE (|$x| $this.v)(5) FROM qv);
LET $f = |$x| $this.v;
SELECT VALUE (SELECT VALUE $f(0) FROM qv) FROM w;
