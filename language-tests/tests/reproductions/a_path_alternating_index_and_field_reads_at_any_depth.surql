/**
[test]
reason = """
	A path that alternates an index and a field broadcasts its rest over each
	step's value. It is compiled once per step however deep it nests, so a
	path twenty levels deep reads its leaf, and a step over a value that is
	not an array applies the rest to the value itself.
"""

[[test.results]]
value = "NONE"

[[test.results]]
value = "'leaf'"

[[test.results]]
value = "[[1, 2], [3]]"

[[test.results]]
value = "1"

*/
LET $v = [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: [{ inner: 'leaf' }] }] }] }] }] }] }] }] }] }] }] }] }] }] }] }] }] }] }] }];
RETURN $v[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner[0].inner;
RETURN [{ a: [1, 2] }, { a: [3] }].a;
RETURN { a: 1 }.a;
