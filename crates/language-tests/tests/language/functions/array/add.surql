/**
[test]

[[test.results]]
value = "[3]"

[[test.results]]
error = "Incorrect arguments for function array::add(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[1, 2, [2, 3]]"

[[test.results]]
value = "[[1, 2], [2, 3]]"

[[test.results]]
value = "[[1, 2], 2, 3]"

[[test.results]]
error = "Incorrect arguments for function array::add(). Expected 2 arguments"

[[test.results]]
value = "[[1, 2], { foo: 1 }]"

*/

RETURN array::add([], 3);
RETURN array::add(3, true);
RETURN array::add([1,2], 2);
RETURN array::add([1,2], 3);
RETURN array::add([1,2], [2,3]);
RETURN array::add([1,2], [[2,3]]);
RETURN array::add([[1,2]], [[2,3]]);
RETURN array::add([[1,2]], [2,3]);
RETURN array::add([[1,2]], 2,3);
RETURN array::add([[1,2]], {foo: 1});
