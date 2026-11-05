/**
[env]
namespace = true
database = true

[test]
reason = "A path rooted at a parameter reads the value the parameter is bound to in the innermost scope that binds it: indexes (including the last element and a negative index), nested fields, closed and open ranges, methods, filters, broadcasts (a mapping step followed by an index), `.*` over plain values, objects and record ids, parenthesised roots, and destructuring over a LET binding; a missing parameter reads NONE; block, FOR, closure-argument and closure-capture bindings shadow outer ones; a local binding shadows a DEFINE PARAM of the same name; $this, $self and $parent read the row, including $this in a subquery's FROM clause and $parent in an idiom-level filter; a record-id parameter fetches and traverses."

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "4"

[[test.results]]
value = "30"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1, 2], [3, 4]]"

[[test.results]]
value = "3"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[10, 20, 30]"

[[test.results]]
value = "[10, 20]"

[[test.results]]
value = "[2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1, 2, 3], [1, 2, 3, 4]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[['a'], ['b', 'c']]"

[[test.results]]
value = "'c'"

[[test.results]]
value = "{ n: 1, t: ['a'] }"

[[test.results]]
value = "[{ n: 1 }, { n: 2 }]"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "7"

[[test.results]]
value = "1"

[[test.results]]
value = "[50]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "100"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ i: 0, x: 1, y: 10 }, { i: 1, x: 2, y: 20 }, { i: 2, x: 3, y: 30 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[1, 3]"

[[test.results]]
value = "[20]"

[[test.results]]
value = "[{ id: pt:1, vals: [5, 6] }]"

[[test.results]]
value = "[{ id: pt:2, vals: [7] }]"

[[test.results]]
value = "[6]"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[[7, 6]]"

[[test.results]]
value = "[[5, 7]]"

[[test.results]]
value = "[{ id: link:l, in: pt:1, out: pt:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "5"

[[test.results]]
value = "[pt:2]"

[[test.results]]
value = "[[7]]"

[[test.results]]
value = "[pt:2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
value = "NONE"

[[test.results]]
value = "4"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['a', 'c']"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "[['a', 'b'], ['c']]"

[[test.results]]
value = "['b', NONE]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ a: [1, 2], b: 3 }"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: pt:1, vals: [5, 6] }, { id: pt:2, vals: [7] }]"

[[test.results]]
value = "[[5, 6], [7]]"

[[test.results]]
value = "[5, 7]"

[[test.results]]
value = "[2, 3]"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "3"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "10"

[[test.results]]
value = "[5]"

[[test.results]]
value = "[[5, 6]]"

[[test.results]]
value = "[[pt:2]]"

[[test.results]]
value = "[[]]"








































































































































*/

LET $big = [[1, 2], [3, 4], { a: { b: [10, 20, 30] } }];
$big[0];
$big[1][1];
$big[2].a.b[2];
$big[5];
$big[2].missing.deeper;
$big[0..2];
$big.len();
$big.first();
$big.last().a.b;
$big[0].map(|$v| $v * 10);
$big[0][WHERE $this > 1];

-- The index expression reads the same parameter it indexes into.
LET $v = [1, 2, 3];
$v[$v[0]];

-- A method that builds a new value leaves the binding as it was.
LET $w = $v.append(4);
[$v, $w];

-- A parameter bound nowhere reads NONE.
$nope[0];
$nope.a.b;

LET $objs = [{ n: 1, t: ['a'] }, { n: 2, t: ['b', 'c'] }];
$objs.n;
$objs.*.n;
$objs.t;
$objs[1].t[1];
$objs[0].{ n, t };
$objs.{ n };
$objs[0].?.n;
$objs[3].?.n;

-- Block scope shadows, then restores.
{
	LET $v = [7, 8, 9];
	$v[0];
};
$v[0];

-- A closure argument shadows a captured binding of the same name; a capture
-- keeps the value it was created with.
array::map([[40, 50]], |$v| $v[1]);
LET $f = |$i: int| $v[$i];
$f(2);
LET $g = |$v: array| $v[0];
$g([100]);
{
	LET $v = [0];
	$f(1);
};

-- A FOR binding shadows an outer one, and the loop body indexes an outer
-- binding by the loop variable.
FOR $i IN [0, 1, 2] {
	CREATE row SET i = $i, x = $v[$i], y = $big[2].a.b[$i];
};
SELECT i, x, y FROM row ORDER BY i;
FOR $v IN [[4, 5]] {
	CREATE shadow SET x = $v[1];
};
SELECT VALUE x FROM shadow;

-- The parameter is indexed by the row value.
SELECT VALUE $v[$this] FROM [0, 2];
SELECT VALUE $big[2].a.b[$this] FROM [1];

-- $this and $parent read the row rather than a binding.
CREATE pt:1 SET vals = [5, 6];
CREATE pt:2 SET vals = [7];
SELECT VALUE $this.vals[1] FROM pt:1;
SELECT VALUE (SELECT VALUE $parent.vals[0] FROM ONLY pt:2) FROM pt:1;
SELECT VALUE (SELECT VALUE [$this.vals[0], $parent.vals[1]] FROM ONLY pt:2) FROM pt:1;
SELECT VALUE [$this.vals[0], (SELECT VALUE $this.vals[0] FROM ONLY pt:2)] FROM pt:1;

-- A record-id parameter is fetched and traversed.
RELATE pt:1->link:l->pt:2;
LET $rid = pt:1;
$rid.vals[0];
$rid->link->pt;
$rid->link->pt.vals;
$rid.{1}(->link->pt);

-- A local binding shadows a DEFINE PARAM of the same name.
DEFINE PARAM $dp VALUE { a: [1, 2] };
$dp.a[1];
LET $dp = { a: [3, 4] };
$dp.a[1];

-- A mapping step followed by a non-mapping one compiles to a broadcast over
-- the parameter.
LET $tags = [{ n: 1, t: ['a', 'b'] }, { n: 2, t: ['c'] }];
$tags.t[0];
$tags.*[0];
$tags.*.t;
$tags.*.t[1];

-- `.*` passes plain elements and objects through, and dereferences record ids.
LET $o = { a: [1, 2], b: 3 };
$o.*;
$o.*.a;
LET $rids = [pt:1, pt:2];
$rids.*;
$rids.*.vals;
$rids.*.vals[0];

-- Open and full ranges, the last element, and a negative index.
$v[1..];
$v[..2];
$v[..];
$v[$];
$v[-1];

-- A parenthesised root.
($v)[0];
($big.last().a).b[0];

-- $self reads the row like $this; in a subquery's FROM clause $this is the
-- enclosing row.
SELECT VALUE $self.vals[0] FROM pt:1;
SELECT VALUE (SELECT VALUE $this FROM $this.vals) FROM pt:1;

-- An idiom-level filter on a record-id parameter's traversal reads $parent as
-- the enclosing row (pt:2, vals [7]), not the parameter (pt:1, vals [5, 6]).
SELECT VALUE $rid->link[WHERE $parent.vals[0] = 7]->pt FROM pt:2;
SELECT VALUE $rid->link[WHERE $parent.vals[0] = 5]->pt FROM pt:2;
