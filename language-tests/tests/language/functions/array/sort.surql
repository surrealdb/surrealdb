/**
[env]
imports = ["datasets/type_order.surql"]

[test]

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::sort(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
value = "[1, 2, 3, 4, 4, 'text']"

[[test.results]]
value = "[1, 2, 3, 4, 4, 'text']"

[[test.results]]
value = "['text', 4, 4, 3, 2, 1]"

[[test.results]]
value = "[1, 2, 3, 4, 4, 'text']"

[[test.results]]
value = "['text', 4, 4, 3, 2, 1]"

[[test.results]]
value = "[1..2, /a/, a:b, (1, 2), {  }, [], u'ffffffff-ffff-ffff-ffff-ffffffffffff', 1d, 1, true, NULL, NONE]"


*/
array::sort([]);
array::sort(3, false);
array::sort([4,2,"text",1,3,4]);
array::sort([4,2,"text",1,3,4], true);
array::sort([4,2,"text",1,3,4], false);
array::sort([4,2,"text",1,3,4], "asc");
array::sort([4,2,"text",1,3,4], "desc");

array::sort($type_array,false);
