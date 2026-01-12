/**
[test]

[[test.results]]
error = "Incorrect arguments for function array::flatten(). Expected 1 argument"

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::flatten(). Argument 1 was the wrong type. Expected `array` but found `'some text'`"

[[test.results]]
value = "[1, 2, 3, 4]"

[[test.results]]
value = "[1, 2, 3, 4, 'SurrealDB', 5, 6, [7, 8]]"

[[test.results]]
value = "[[1], [2]]"

*/
array::flatten();
array::flatten([]);
array::flatten("some text");
array::flatten([[1,2], [3,4]]);
array::flatten([[1,2], [3, 4], 'SurrealDB', [5, 6, [7, 8]]]);
array::flatten([[[1]],[[2]]]);
