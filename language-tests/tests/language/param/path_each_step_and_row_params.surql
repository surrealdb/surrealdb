/**
[env]
namespace = true
database = true

[test]
reason = "A `.*` anywhere in a path returns an object, or an array holding no record id, unchanged, and dereferences only the record ids an array holds at its top level: after the first step of a path over a binding, consecutively at its start, in a path rooted in the row or in $this, over a DEFINE PARAM or a function result, and at the start of the body a mapping step applies per element; a scalar or missing value reads NONE. `.*` fetches an object-keyed record id before the next field is read, so the field comes from the stored record, while a field read straight off the id answers the key component. $this and $self read the row in a write and a read and NONE outside one. $parent reads its binding first and the enclosing row second, both ahead of a DEFINE PARAM of the same name, which it reads only outside any row."

[[test.results]]
value = "[{ id: pt:1, vals: [5, 6] }]"

[[test.results]]
value = "[{ id: pt:2, vals: [7] }]"

[[test.results]]
value = "[{ id: link:l, in: pt:1, out: pt:2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['a', 'b']"

[[test.results]]
value = "[{ n: 1, name: 'a' }, { n: 2, name: 'b' }]"

[[test.results]]
value = "['a', 'b']"

[[test.results]]
value = "1"

[[test.results]]
value = "[{ id: pt:1, vals: [5, 6] }, { id: pt:2, vals: [7] }]"

[[test.results]]
value = "[[5, 6], [7]]"

[[test.results]]
value = "[1, { id: pt:1, vals: [5, 6] }, { k: 1 }, [pt:2]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "'a'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[{ a: 1 }, { a: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[pt:1], 4]"

[[test.results]]
value = "[[[5, 6]], NONE]"

[[test.results]]
value = "[{ id: doc:1, items: [{ name: 'a' }, { name: 'b' }], n: 3, rids: [pt:1, pt:2] }]"

[[test.results]]
value = "[['a', 'b']]"

[[test.results]]
value = "[[{ name: 'a' }, { name: 'b' }]]"

[[test.results]]
value = "[[{ name: 'a' }, { name: 'b' }]]"

[[test.results]]
value = "[[{ id: pt:1, vals: [5, 6] }, { id: pt:2, vals: [7] }]]"

[[test.results]]
value = "[[[5, 6], [7]]]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[['a', 'b']]"

[[test.results]]
value = "[[[5, 6], [7]]]"

[[test.results]]
value = "[{ id: doc:1, items: [{ name: 'a' }, { name: 'b' }], n: 3, rids: [pt:1, pt:2] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['x']"

[[test.results]]
value = "[[7]]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[NONE, NONE, [7]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[[1, 2], [3]]"

[[test.results]]
value = "[[1, 2], [3]]"

[[test.results]]
value = "[[NONE, NONE], [NONE]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5, 9, NONE]"

[[test.results]]
value = "[5, 9, NONE]"

[[test.results]]
value = "[{ a: 'stored', id: ok:{ a: 1 } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['stored']"

[[test.results]]
value = "[1]"

[[test.results]]
value = "'stored'"

[[test.results]]
value = "1"

[[test.results]]
value = "[{ id: pt:3, vals: [1, 2] }]"

[[test.results]]
value = "[[2, [1, 2]]]"

[[test.results]]
value = "[[1, 2]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "[[pt:2]]"

[[test.results]]
value = "[[pt:2]]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[[]]"

[[test.results]]
value = "[[pt:2]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "42"

[[test.results]]
value = "[[pt:2]]"
*/

CREATE pt:1 SET vals = [5, 6];
CREATE pt:2 SET vals = [7];
RELATE pt:1->link:l->pt:2;

-- A `.*` after the first step of a path over a binding.
LET $obj = {
	items: [{ name: 'a', n: 1 }, { name: 'b', n: 2 }],
	rids: [pt:1, pt:2],
	mixed: [1, pt:1, { k: 1 }, [pt:2]],
	n: 3,
	nested: { inner: { a: 1 } }
};
$obj.items.*.name;
$obj.items.*;
$obj.items.*.*.name;
$obj.nested.*.inner.a;
$obj.rids.*;
$obj.rids.*.vals;
$obj.mixed.*;
$obj.n.*;
$obj.n.*.x;
$obj.missing.*;
$obj.items.*.n[0];
$obj.items[0].*.name;

-- Consecutive leading `.*` over a binding.
LET $arr = [{ a: 1 }, { a: 2 }];
$arr.*.*.a;
$arr.*.*;
LET $nested = [[pt:1], 4];
$nested.*;
$nested.*.vals;

-- A path rooted in the row.
CREATE doc:1 SET items = [{ name: 'a' }, { name: 'b' }], rids = [pt:1, pt:2], n = 3;
SELECT VALUE items.*.name FROM doc:1;
SELECT VALUE items.* FROM doc:1;
SELECT VALUE items.*.* FROM doc:1;
SELECT VALUE rids.* FROM doc:1;
SELECT VALUE rids.*.vals FROM doc:1;
SELECT VALUE n.* FROM doc:1;
SELECT VALUE $this.items.*.name FROM doc:1;
SELECT VALUE $this.rids.*.vals FROM doc:1;
SELECT VALUE $this.* FROM doc:1;

-- A root that is neither a binding nor the row.
DEFINE PARAM $dobj VALUE { items: [{ name: 'x' }], rids: [pt:2] };
$dobj.items.*.name;
$dobj.rids.*.vals;
array::concat($arr, [{ a: 3 }]).*.a;
array::concat($arr, [pt:2]).*.vals;

-- A `.*` inside the body a mapping step applies per element.
LET $groups = [{ t: [{ n: 1 }, { n: 2 }] }, { t: [{ n: 3 }] }];
$groups.t.*.n;
$groups.*.t.*.n;
$groups.*.t.*.n[0];
LET $grs = [pt:1, { vals: [9] }, 4];
$grs.*.vals[0];
$grs.*.*.vals[0];

-- `.*` dereferences a record id before the next field is read: a field read
-- straight off an object-keyed record id answers the key component instead.
CREATE ok:{ a: 1 } SET a = 'stored';
LET $oks = [ok:{ a: 1 }];
$oks.*.a;
$oks.a;
ok:{ a: 1 }.*.a;
ok:{ a: 1 }.a;

-- $this and $self read the row; outside one they read NONE.
CREATE pt:3 SET vals = [1, 2];
UPDATE pt:3 SET copy = $this.vals[1], whole = $self.vals.* RETURN VALUE [copy, whole];
SELECT VALUE [$this.vals[0], $self.vals[$]] FROM pt:3;
RETURN $this.vals;
RETURN [$this, $self];

-- $parent: a binding wins, the enclosing row stands in when there is none.
SELECT VALUE ->link[WHERE $parent.vals[1] = 6]->pt FROM pt:1;
SELECT VALUE ->link[WHERE $parent.vals.* = [5, 6]]->pt FROM pt:1;
SELECT VALUE ->link[WHERE $parent.vals[1] = 7]->pt FROM pt:1;
SELECT VALUE (SELECT VALUE ->link[WHERE $parent.vals[0] = 7]->pt FROM ONLY pt:1) FROM pt:2;
SELECT VALUE (SELECT VALUE ->link[WHERE $parent.vals[0] = 5]->pt FROM ONLY pt:1) FROM pt:2;
RETURN $parent.vals[0];
DEFINE PARAM $parent VALUE { vals: [42] };
RETURN $parent.vals[0];
SELECT VALUE ->link[WHERE $parent.vals[0] = 5]->pt FROM pt:1;
