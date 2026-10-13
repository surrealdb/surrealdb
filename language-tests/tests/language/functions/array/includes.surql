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
error = "Incorrect arguments for function array::includes(). Argument 1 was the wrong type. Expected `array` but found `{ bar: 2, foo: 1 }`"
*/
array::includes([1, 2, 3], 2);
array::includes([1, 2, 3], 4);
array::includes([1, 2, 3], |$v| $v > 2);
array::includes([1, 2, 3], |$v| $v > 5);
array::includes([0, false, NONE]);
array::includes([0, false, 1]);
array::includes({ foo: 1, bar: 2 }, 1);
