/**
[test]

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
error = "Incorrect arguments for function array::some(). Argument 1 was the wrong type. Expected `array` but found `123`"
*/
array::some([1, 2, 3], |$v| $v > 2);
array::some([1, 2, 3], |$v| $v > 5);
array::some([1, 2, 3], 2);
array::some([1, 2, 3], 4);
array::some([0, false, NONE]);
array::some([0, false, 1]);
array::some(123, |$v| $v > 2);
