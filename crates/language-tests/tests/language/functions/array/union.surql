/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::union(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
value = "[1, 2, 6, 3, 4, 5]"

[[test.results]]
# TODO: Look into the result her sometimes being inconsistant, sometimes the normal `1` is being omitted.
value = "[{  }, 1d, 'foo', 1f, 1h, '', 1dec, 1]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[1]"

*/
array::union([], []);
array::union(3, true);
array::union([1,2,1,6], [1,3,4,5,6]);
array::union([{},1d,"foo",1f], [{},1h,"",1dec,1]);
array::union([1,1,1], []);
array::union([],[1,1,1]);
array::union([1,1,1],[1,1,1]);
