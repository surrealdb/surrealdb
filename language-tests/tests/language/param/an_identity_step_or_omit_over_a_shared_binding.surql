/**
[env]
namespace = true
database = true

[test]
reason = "An OMIT over a bound object answers the object without the omitted fields and leaves the binding as it was. An identity `.*` answers a bound set, or an object or array a path step answered, unchanged, and a bound array or set holding a record id with each record fetched; neither changes the binding. An array longer than one fuel quantum is walked across yields, and a record id past the first quantum is still fetched."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, nested: { b: 2, c: 3 } }, { nested: { c: 3 } }]"

[[test.results]]
value = "[{ a: 1, nested: { b: 2, c: 3 } }, { nested: { c: 3 } }, { a: 1, nested: { b: 2, c: 3 } }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{1, 2, 3}, 'set', {1, 2, 3}]"

[[test.results]]
value = "[{1, 2, 3}, 'set', {1, 2, 3}]"

[[test.results]]
value = "[{ id: pt:1, x: 1 }]"

[[test.results]]
value = "[{ id: pt:2, x: 2 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{1, 2}, 'set', [pt:1, pt:2]]"

[[test.results]]
value = "[{1, 2}, {{ id: pt:1, x: 1 }, { id: pt:2, x: 2 }}, [pt:1, pt:2]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: [1, 2] }, [3, 1, 2], [1], { ids: [pt:1], inner: { a: 1, b: [1, 2] }, list: [3, 1, 2] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ a: 1, b: [1, 2], c: 3 }, { a: 1, b: [1, 2] }, { ids: [pt:1], inner: { a: 1, b: [1, 2] }, list: [3, 1, 2] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[6000, 4095, 5999, 6000]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[5001, { id: pt:2, x: 2 }, 4999, pt:2]"

[[test.results]]
value = "[5001, { id: pt:2, x: 2 }, pt:2]"

*/

-- OMIT over a bound object, top level and in a block.
LET $shared = { a: 1, nested: { b: 2, c: 3 } };
LET $omitted = SELECT * OMIT a, nested.b FROM ONLY $shared;
RETURN [$shared, $omitted];
{
	LET $shared = { a: 1, nested: { b: 2, c: 3 } };
	LET $omitted = SELECT * OMIT a, nested.b FROM ONLY $shared;
	RETURN [$shared, $omitted, $shared];
};

-- `.*` over a bound set with no record id, top level and in a block.
LET $set = {3, 1, 2};
RETURN [$set.*, type::of($set.*), $set];
{
	LET $set = {3, 1, 2};
	LET $each = $set.*;
	RETURN [$each, type::of($each), $set];
};

-- `.*` over a bound set of record ids fetches each record.
CREATE pt:1 SET x = 1;
CREATE pt:2 SET x = 2;
LET $ids = <set> [pt:2, pt:1];
RETURN [$ids.*.x, type::of($ids), <array> $ids];
{
	LET $ids = <set> [pt:2, pt:1];
	RETURN [$ids.*.x, $ids.*, <array> $ids];
};

-- `.*` over what a path step answered, then a change to that answer.
LET $w = { inner: { a: 1, b: [1, 2] }, list: [3, 1, 2], ids: [pt:1] };
RETURN [$w.inner.*, $w.list.*, $w.ids.*.x, $w];
LET $e = $w.inner.*;
RETURN [object::extend($e, { c: 3 }), $e, $w];

-- `.*` over arrays longer than one fuel quantum.
LET $plain = array::range(0, 6000);
RETURN [array::len($plain.*), ($plain.*)[4095], ($plain.*)[5999], array::len($plain)];
LET $late = array::concat(array::range(0, 5000), [pt:2]);
RETURN [array::len($late.*), array::last($late.*), ($late.*)[4999], array::last($late)];
{
	LET $late = array::concat(array::range(0, 5000), [pt:2]);
	RETURN [array::len($late.*), array::last($late.*), array::last($late)];
};
