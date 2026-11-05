/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A part after a run of `.*` applies to each element, over a collection and over
a lone value alike. Each `.*` before an index adds a fixed amount of code, so
twenty of them before `[0]` compile and answer, where a body copied once per
`.*` overflowed the VM's registers with `IR table full: vm registers`.
"""

[[test.results]]
value = "[{ id: pp:1, name: 'a' }]"

[[test.results]]
value = "[{ id: pp:2, name: 'b' }]"

[[test.results]]
value = "[{ id: g:1, nested: [[pp:1], [pp:2, 5]], obj: { a: { b: [1, 2] } }, people: [pp:1, pp:2] }]"

[[test.results]]
value = "[[NONE, NONE]]"

[[test.results]]
value = "[['a', 'b']]"

[[test.results]]
value = "[[[NONE], [NONE, NONE]]]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[[[NONE, NONE], [NONE, NONE]], [[NONE, NONE]]]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[NONE, NONE]"

[[test.results]]
value = "[[NONE]]"
*/
CREATE pp:1 SET name = 'a';
CREATE pp:2 SET name = 'b';
CREATE g:1 SET people = [pp:1, pp:2], nested = [[pp:1], [pp:2, 5]], obj = { a: { b: [1, 2] } };
SELECT VALUE people.*[0] FROM g:1;
SELECT VALUE people.*.name FROM g:1;
SELECT VALUE nested.*.*[0] FROM g:1;
SELECT VALUE obj.*.*[1] FROM g:1;
RETURN [[[1, 2], [3, 4]], [[5, 6]]].*.*.*[0];
RETURN pp:1.*[0];
RETURN [pp:1, 3].*.*[0];
RETURN [[1]].*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*.*[0];
