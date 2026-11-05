/**
[env]
namespace = true
database = true

[test]
reason = "An operation that changes a value read from a binding answers a changed copy and leaves the binding as it was. A top-level LET, a LET inside a block, a FOR binding, a closure argument and a function argument each keep their value after a sort, a push, an append, an object extension, an OMIT, an identity `.*` and a `+=` in a SET clause over a value read from them."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[3, 1, 2], [1, 2, 3]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[3, 1, 2], [3, 1, 2, 4]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[3, 1, 2], [3, 1, 2, 5]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[3, 1, 2], [3, 1, 2], [1, 2, 3], [3, 1, 2]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ x: 1, y: { z: 2 } }, { w: 3, x: 1, y: { z: 2 } }]"

[[test.results]]
value = "[{ x: 1 }]"

[[test.results]]
value = "{ x: 1, y: { z: 2 } }"

[[test.results]]
value = "[{ arr: [3, 1, 2, 6], id: t:1 }]"

[[test.results]]
value = "[{ arr: [3, 1, 2, 6, 3, 1, 2], id: t:1 }]"

[[test.results]]
value = "[[3, 1, 2], [3, 1, 2, 6, 3, 1, 2]]"

[[test.results]]
value = "[[9, 7, 8], [7, 8, 9], [9, 7, 8, 6], [9, 7, 8, 5], [9, 7, 8], [7, 8, 9], [9, 7, 8, 4], [9, 7, 8]]"

[[test.results]]
value = "[{ x: 1, y: { z: 2 } }, { w: 3, x: 1, y: { z: 2 } }, [{ x: 1 }], { x: 1, y: { z: 2 } }, { x: 1, y: { z: 2 } }]"

[[test.results]]
value = "[[3, 1, 2], { x: 1, y: { z: 2 } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ appended: [3, 1, 2, 5], each: [3, 1, 2], orig: [3, 1, 2], pushed: [3, 1, 2, 4], sorted: [1, 2, 3] }]"

[[test.results]]
value = "[{ changed: [3, 1, 2, 9], orig: [3, 1, 2] }]"

[[test.results]]
value = "[[[1, 2, 3], [3, 1, 2, 4], [3, 1, 2, 5], [3, 1, 2], [3, 1, 2]]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1, 2, 3], [3, 1, 2, 4], [3, 1, 2]]"

[[test.results]]
value = "[3, 1, 2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ w: 3, x: 1, y: { z: 2 } }, { x: 1, y: { z: 2 } }, { x: 1, y: { z: 2 } }]"

[[test.results]]
value = "{ x: 1, y: { z: 2 } }"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1, 2, 3], [3, 1, 2, 4], [3, 1, 2, 9], [3, 1, 2], [3, 1, 2]]"

[[test.results]]
value = "[3, 1, 2]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ w: 3, x: 1, y: { z: 2 } }, [{ x: 1 }], { x: 1, y: { z: 2 } }, { x: 1, y: { z: 2 } }]"

[[test.results]]
value = "{ x: 1, y: { z: 2 } }"
*/

-- A top-level LET.
LET $a = [3, 1, 2];
LET $b = array::sort($a);
RETURN [$a, $b];
LET $c = array::push($a, 4);
RETURN [$a, $c];
LET $d = $a.append(5);
RETURN [$a, $d];
LET $e = $a.*;
RETURN [$a, $e, array::sort($e), $a];
LET $o = { x: 1, y: { z: 2 } };
LET $p = object::extend($o, { w: 3 });
RETURN [$o, $p];
SELECT * OMIT y FROM [$o];
RETURN $o;
CREATE t:1 SET arr = $a, arr += 6;
UPDATE t:1 SET arr += $a;
RETURN [$a, t:1.arr];

-- A LET inside a block.
{
	LET $a = [9, 7, 8];
	LET $b = array::sort($a);
	LET $c = array::push($a, 6);
	LET $d = $a.append(5);
	LET $e = $a.*;
	LET $f = array::sort($e);
	LET $g = (CREATE ONLY t:2 SET arr = $a, arr += 4 RETURN VALUE arr);
	RETURN [$a, $b, $c, $d, $e, $f, $g, $a];
};
{
	LET $o = { x: 1, y: { z: 2 } };
	LET $e = object::extend($o, { w: 3 });
	LET $omitted = SELECT * OMIT y FROM [$o];
	RETURN [$o, $e, $omitted, $o.*, $o];
};
RETURN [$a, $o];

-- A FOR binding.
FOR $v IN [[3, 1, 2]] {
	CREATE forrow:1 SET sorted = array::sort($v), pushed = array::push($v, 4), appended = $v.append(5), each = $v.*, orig = $v;
	CREATE forrow:2 SET changed = $v, changed += 9, orig = $v;
};
SELECT sorted, pushed, appended, each, orig FROM forrow:1;
SELECT changed, orig FROM forrow:2;

-- A closure argument.
array::map([[3, 1, 2]], |$v| [array::sort($v), array::push($v, 4), $v.append(5), $v.*, $v]);
LET $f = |$v: array| [array::sort($v), array::push($v, 4), $v];
$f($a);
RETURN $a;
LET $g = |$o: object| [object::extend($o, { w: 3 }), $o.*, $o];
$g($o);
RETURN $o;

-- A function argument.
DEFINE FUNCTION fn::change($v: array) {
	LET $s = array::sort($v);
	LET $p = array::push($v, 4);
	LET $w = (CREATE ONLY fnrow:1 SET w = $v, w += 9 RETURN VALUE w);
	RETURN [$s, $p, $w, $v.*, $v];
};
fn::change($a);
RETURN $a;
DEFINE FUNCTION fn::change_object($o: object) {
	LET $e = object::extend($o, { w: 3 });
	LET $omitted = SELECT * OMIT y FROM [$o];
	RETURN [$e, $omitted, $o.*, $o];
};
fn::change_object($o);
RETURN $o;
