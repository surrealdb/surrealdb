/**
[env]
namespace = true
database = true

[test]
reason = "A numeric JSON Pointer reference token names an array index, and every `PATCH` operation must resolve it as one. Reading it as a member name makes the pointer miss the element it names: `replace` writes the operand into a member spelled after the index on every element of the array instead of into the one it addressed, `change` finds an array where it needs the string it was pointing at and silently does nothing, and `add` through an intermediate index lands the same way. The same token names the member of that spelling when the container is an object, which is the reading `index[0]` and `obj.0` share."

[[test.results]]
value = "[{ details: [{ isbn: 'a', title: 'A' }, { isbn: 'b', title: 'B' }], id: book:1, tags: ['test'] }]"

# `replace` addresses a member of one element, leaving its siblings alone.
[[test.results]]
value = "[{ details: [{ isbn: 'z', title: 'A' }, { isbn: 'b', title: 'B' }], id: book:1, tags: ['test'] }]"

# `replace` at an element replaces that element.
[[test.results]]
value = "[{ details: [{ isbn: 'z', title: 'A' }, 'gone'], id: book:1, tags: ['test'] }]"

# `change` applies its diff to the string the pointer selects.
[[test.results]]
value = "[{ details: [{ isbn: 'z', title: 'A' }, 'gone'], id: book:1, tags: ['text'] }]"

# `add` through an intermediate index adds to that element only.
[[test.results]]
value = "[{ details: [{ extra: 'E', isbn: 'z', title: 'A' }, 'gone'], id: book:1, tags: ['text'] }]"

# Indexes nest.
[[test.results]]
value = "[{ a: [[1, 2], [3, 4]], id: nested:1 }]"

[[test.results]]
value = "[{ a: [[1, 99], [3, 4]], id: nested:1 }]"

# On an object the same token names the member of that spelling. A token
# spelled any other way — `007`, `+3`, `-1` — is a member name and nothing
# else, so those members stay addressable.
[[test.results]]
value = "[{ id: keys:1, m: { '+3': 'plus', '-1': 'neg', '0': 'zero', '007': 'bond', '7': 'seven' } }]"

[[test.results]]
value = "[{ id: keys:1, m: { '+3': 'plus', '-1': 'N', '0': 'Z', '7': 'seven' } }]"

# What `value::diff` emits for an array, `value::patch` can apply back.
[[test.results]]
value = "NONE"

[[test.results]]
value = "true"

# A `copy` or `move` destination is an `add`, so a position in an array is one
# the value takes rather than one it overwrites: nothing is displaced.
[[test.results]]
value = "[{ id: seq:1, list: ['a', 'c'], v: 'b' }]"

[[test.results]]
value = "[{ id: seq:1, list: ['a', 'b', 'c'], v: 'b' }]"

# A `move` removes its source before writing its destination, so a position in
# the same array counts against what the removal left behind.
[[test.results]]
value = "[{ id: seq:2, list: ['a', 'b', 'c'] }]"

[[test.results]]
value = "[{ id: seq:2, list: ['b', 'c', 'a'] }]"

# `-` names the position past the last element.
[[test.results]]
value = "[{ id: seq:2, list: ['b', 'c', 'a', 'end'] }]"

# A `replace` reaching past the last element changes nothing, so it is refused
# rather than reported as applied, and `-` never names an element to overwrite.
[[test.results]]
error = "The JSON Patch contains invalid operations. Failed to parse JSON patch structure: index 9 is out of bounds for array of length 4"

[[test.results]]
error = "The JSON Patch contains invalid operations. Failed to parse JSON patch structure: index 4 is out of bounds for array of length 4"

[[test.results]]
value = "[{ id: seq:2, list: ['b', 'c', 'a', 'end'] }]"
*/

CREATE book:1 SET details = [{ isbn: 'a', title: 'A' }, { isbn: 'b', title: 'B' }], tags = ['test'];
UPDATE book:1 PATCH [{ op: 'replace', path: '/details/0/isbn', value: 'z' }];
UPDATE book:1 PATCH [{ op: 'replace', path: '/details/1', value: 'gone' }];
UPDATE book:1 PATCH [{ op: 'change', path: '/tags/0', value: '@@ -1,4 +1,4 @@\n te\n-s\n+x\n t\n' }];
UPDATE book:1 PATCH [{ op: 'add', path: '/details/0/extra', value: 'E' }];

CREATE nested:1 SET a = [[1, 2], [3, 4]];
UPDATE nested:1 PATCH [{ op: 'replace', path: '/a/0/1', value: 99 }];

CREATE keys:1 SET m = { '0': 'zero', '007': 'bond', '7': 'seven', '-1': 'neg', '+3': 'plus' };
UPDATE keys:1 PATCH [
    { op: 'replace', path: '/m/0', value: 'Z' },
    { op: 'remove', path: '/m/007' },
    { op: 'replace', path: '/m/-1', value: 'N' }
];

LET $a = { list: [{ n: 1 }, { n: 2 }] };
RETURN value::patch($a, value::diff($a, { list: [{ n: 1 }, { n: 7 }] })) = { list: [{ n: 1 }, { n: 7 }] };

CREATE seq:1 SET list = ['a', 'c'], v = 'b';
UPDATE seq:1 PATCH [{ op: 'copy', path: '/list/1', from: '/v' }];

CREATE seq:2 SET list = ['a', 'b', 'c'];
UPDATE seq:2 PATCH [{ op: 'move', path: '/list/2', from: '/list/0' }];
UPDATE seq:2 PATCH [{ op: 'add', path: '/list/-', value: 'end' }];
UPDATE seq:2 PATCH [{ op: 'replace', path: '/list/9', value: 'x' }];
UPDATE seq:2 PATCH [{ op: 'replace', path: '/list/-', value: 'x' }];
SELECT * FROM seq:2;
