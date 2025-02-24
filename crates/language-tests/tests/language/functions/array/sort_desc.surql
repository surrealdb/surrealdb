/**
[env]
imports = ["datasets/type_order.surql"]

[test]

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::sort::desc(). Expected 1 argument."

[[test.results]]
value = "['text', 4, 4, 3, 2, 1]"

[[test.results]]
value = "['text', 4, 4, 3, 2, 1]"

[[test.results]]
value = "['text', 4, 4, 3, 2, 1]"

[[test.results]]
value = "[NONE, NULL, true, 1, 1d, u'ffffffff-ffff-ffff-ffff-ffffffffffff', [], {  }, (1, 2), a:b, |a:1..2|, /a/, 1..2, |$a: any| {  }]"

[[test.results]]
value = "[|$a: any| {  }, 1..2, /a/, |a:1..2|, a:b, (1, 2), {  }, [], u'ffffffff-ffff-ffff-ffff-ffffffffffff', 1d, 1, true, NULL, NONE]"

*/

array::sort::desc([]);
array::sort::desc(3, false);
array::sort::desc([4,2,"text",1,3,4]);
array::sort::desc([4,2,"text",1,3,4]);
array::sort::desc([4,2,"text",1,3,4]);

$type_array;
array::sort::desc($type_array);
