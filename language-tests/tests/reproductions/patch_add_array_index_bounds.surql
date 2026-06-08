/**
[env]
namespace = true
database = true

[test]
reason = "RFC 6902 §4.1: the index of a JSON Patch `add` operation MUST NOT be greater than the number of elements in the array. Previously, an out-of-bounds index would silently append; the spec requires an error."

[[test.results]]
value = "[{ id: list:1, items: ['a', 'b'] }]"

# An index equal to the array length is the spec's append form and must succeed.
[[test.results]]
value = "[{ id: list:1, items: ['a', 'b', 'c'] }]"

# An index greater than the array length must error and leave the record unchanged.
[[test.results]]
error = "The JSON Patch contains invalid operations. Failed to parse JSON patch structure: index 9 is out of bounds for array of length 3"

[[test.results]]
value = "[{ id: list:1, items: ['a', 'b', 'c'] }]"
*/

CREATE list:1 SET items = ['a', 'b'];
UPDATE list:1 PATCH [{ op: 'add', path: '/items/2', value: 'c' }];
UPDATE list:1 PATCH [{ op: 'add', path: '/items/9', value: 'd' }];
SELECT * FROM list:1;
