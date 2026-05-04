/**
[test]
reason = "diff-match-patch (DMP) coverage: string `change` ops from value::diff and value::patch application, method syntax, and non-DMP `replace` when types differ"

[[test.results]]
value = '''[{ op: 'change', path: '/note', value: '@@ -1,5 +1,11 @@\n-shor\n+longer tex\n t\n' }]'''

[[test.results]]
value = "{ note: 'longer text' }"

[[test.results]]
value = '''[{ op: 'change', path: '/note', value: '@@ -1,5 +1,11 @@\n-shor\n+longer tex\n t\n' }]'''

[[test.results]]
value = "{ note: 'longer text' }"

[[test.results]]
value = "[]"

[[test.results]]
value = '''[{ op: 'change', path: '/a/b/c', value: '@@ -1,1 +1,2 @@\n-x\n+yz\n' }]'''

[[test.results]]
value = "{ a: { b: { c: 'yz' } } }"

[[test.results]]
value = '''[{ op: 'change', path: '/a', value: '@@ -1,3 +1,4 @@\n foo\n+t\n' }, { op: 'change', path: '/b', value: '@@ -1,3 +1,3 @@\n ba\n-r\n+z\n' }]'''

[[test.results]]
value = "{ a: 'foot', b: 'baz' }"

[[test.results]]
value = "[{ op: 'replace', path: '/id', value: 5 }]"

[[test.results]]
value = "{ id: 5 }"

[[test.results]]
value = '''[{ op: 'replace', path: '/n', value: 2 }, { op: 'change', path: '/s', value: '@@ -1,2 +1,2 @@\n h\n-i\n+o\n' }]'''

[[test.results]]
value = "{ n: 2, s: 'ho' }"

[[test.results]]
value = '''[{ op: 'change', path: '/t', value: '@@ -1,4 +1,5 @@\n caf%C3%A9\n+s\n' }]'''

[[test.results]]
value = "{ t: 'cafés' }"

[[test.results]]
value = '''[{ op: 'change', path: '/s', value: '@@ -4,8 +4,14 @@\n e1%0Aline2\n+%0Aline3\n' }]'''

[[test.results]]
value = '''{ s: 'line1\nline2\nline3' }'''

[[test.results]]
value = '''[{ op: 'change', path: '/m', value: '@@ -0,0 +1,1 @@\n+x\n' }]'''

[[test.results]]
value = "{ m: 'x' }"

[[test.results]]
value = '''[{ op: 'change', path: '/items/1', value: '@@ -1,4 +1,5 @@\n-bet\n+gamm\n a\n' }]'''

[[test.results]]
value = "{ note: 'bb' }"

[[test.results]]
value = "[{ op: 'replace', path: '/x', value: ['a', 'b'] }]"

[[test.results]]
value = "{ x: ['a', 'b'] }"

[[test.results]]
value = '''[{ op: 'change', path: '/e', value: '@@ -1,1 +1,2 @@\n %F0%9F%91%8B\n+!\n' }]'''

[[test.results]]
value = "{ e: '👋!' }"

[[test.results]]
value = "[]"

[[test.results]]
error = "The JSON Patch contains invalid operations. Failed to parse JSON patch structure: InvalidInput"
*/

-- Basic nested string: DMP emits op `change` with unified diff text
RETURN value::diff({ note: 'short' }, { note: 'longer text' });

RETURN value::patch({ note: 'short' }, value::diff({ note: 'short' }, { note: 'longer text' }));

-- Method call form (same as value::diff / value::patch)
RETURN { note: 'short' }.diff({ note: 'longer text' });

RETURN { note: 'short' }.patch({ note: 'short' }.diff({ note: 'longer text' }));

-- Identical nested strings → empty list
RETURN value::diff({ msg: 'same' }, { msg: 'same' });

-- Deep path
RETURN value::diff({ a: { b: { c: 'x' } } }, { a: { b: { c: 'yz' } } });

RETURN value::patch(
	{ a: { b: { c: 'x' } } },
	value::diff({ a: { b: { c: 'x' } } }, { a: { b: { c: 'yz' } } })
);

-- Two differing string fields → two `change` operations
RETURN value::diff({ a: 'foo', b: 'bar' }, { a: 'foot', b: 'baz' });

RETURN value::patch({ a: 'foo', b: 'bar' }, value::diff({ a: 'foo', b: 'bar' }, { a: 'foot', b: 'baz' }));

-- Non-DMP path: string value replaced by number (`replace`, not `change`)
RETURN value::diff({ id: '5' }, { id: 5 });

RETURN value::patch({ id: '5' }, value::diff({ id: '5' }, { id: 5 }));

-- Mix `replace` (number) and `change` (string) in one diff
RETURN value::diff({ n: 1, s: 'hi' }, { n: 2, s: 'ho' });

RETURN value::patch({ n: 1, s: 'hi' }, value::diff({ n: 1, s: 'hi' }, { n: 2, s: 'ho' }));

-- Unicode text (still two Surreal strings → DMP)
RETURN value::diff({ t: 'café' }, { t: 'cafés' });

RETURN value::patch({ t: 'café' }, value::diff({ t: 'café' }, { t: 'cafés' }));

-- Multi-line string field
RETURN value::diff({ s: "line1\nline2" }, { s: "line1\nline2\nline3" });

RETURN value::patch({ s: "line1\nline2" }, value::diff({ s: "line1\nline2" }, { s: "line1\nline2\nline3" }));

-- Empty string growing content
RETURN value::diff({ m: '' }, { m: 'x' });

RETURN value::patch({ m: '' }, value::diff({ m: '' }, { m: 'x' }));

-- Array of strings: diff uses `/items/<index>`; patch of `change` on array cells is not asserted here
RETURN value::diff({ items: ['alpha', 'beta'] }, { items: ['alpha', 'gamma'] });

-- Combined patch: remove a field then DMP `change` on a remaining string
RETURN value::patch(
	{ keep: true, note: 'aa' },
	array::concat(
		[{ op: 'remove', path: '/keep' }],
		value::diff({ note: 'aa' }, { note: 'bb' })
	)
);

-- Structural change at nested path: `replace` when value is no longer a string
RETURN value::diff({ x: 'ab' }, { x: ['a', 'b'] });

RETURN value::patch({ x: 'ab' }, value::diff({ x: 'ab' }, { x: ['a', 'b'] }));

-- Supplementary-plane (emoji) strings still use DMP `change`
RETURN value::diff({ e: '👋' }, { e: '👋!' });

RETURN value::patch({ e: '👋' }, value::diff({ e: '👋' }, { e: '👋!' }));

-- Identical multi-line field: no ops
RETURN value::diff({ s: "a\nb" }, { s: "a\nb" });

-- Invalid `change` payload (not unified-diff text) must error
RETURN value::patch({ x: 'hello' }, [{ op: 'change', path: '/x', value: 'plain-wrong' }]);
