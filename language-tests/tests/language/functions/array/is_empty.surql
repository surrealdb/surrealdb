/**
[test]

[[test.results]]
error = "Incorrect arguments for function array::is_empty(). Expected 1 argument"

[[test.results]]
error = "Incorrect arguments for function array::is_empty(). Argument 1 was the wrong type. Expected `array` but found `'array'`"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/
array::is_empty();
array::is_empty("array");
array::is_empty([]);
array::is_empty([1,2,3,4,5]);
