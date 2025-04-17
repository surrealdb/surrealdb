/**
[test]

[[test.results]]
value = "[3]"

[[test.results]]
value = "[1, 3]"

[[test.results]]
value = "[1, NULL]"

[[test.results]]
value = "[1, NONE]"

[[test.results]]
error = "Incorrect arguments for function array::append(). Argument 1 was the wrong type. Expected `array` but found `NULL`"

[[test.results]]
error = "Incorrect arguments for function array::append(). Argument 1 was the wrong type. Expected `array` but found `NONE`"

[[test.results]]
error = "Incorrect arguments for function array::append(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
value = "[1, 2, [2, 3]]"

*/
RETURN array::append([], 3);
RETURN array::append([1], 3);
RETURN array::append([1], NULL);
RETURN array::append([1], NONE);
RETURN array::append(NULL, 3);
RETURN array::append(NONE, 3);
RETURN array::append(3, true);
RETURN array::append([1,2], [2,3]);
