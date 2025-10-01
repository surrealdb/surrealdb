/**
[env]
imports = ["datasets/type_order.surql"]

[test]

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::sort::asc(). Expected 1 argument"

[[test.results]]
value = "[1, 2, 3, 4, 4, 'text']"

[[test.results]]
value = "[1, 2, 3, 4, 4, 'text']"

[[test.results]]
value = "[1, 2, 3, 4, 4, 'text']"

[[test.results]]
value = "[NONE, NULL, true, 1, 1d, u'ffffffff-ffff-ffff-ffff-ffffffffffff', [], {  }, (1, 2), a:b, /a/, 1..2]"

*/

array::sort::asc([]);
array::sort::asc(3, false);
array::sort::asc([4,2,"text",1,3,4]);
array::sort::asc([4,2,"text",1,3,4]);
array::sort::asc([4,2,"text",1,3,4]);
// reverse the type_array so that sort actually needs to do something.
array::sort::asc(array::reverse($type_array));
