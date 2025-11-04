/**
[test]

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
error = "Incorrect arguments for function array::any(). Argument 1 was the wrong type. Expected `array` but found `'some text'`"

[[test.results]]
value = "true"


*/
RETURN array::any([]);
RETURN array::any([false]);
RETURN array::any([NULL]);
RETURN array::any([NONE]);
RETURN array::any([[]]);

RETURN array::any([true]);
RETURN array::any("some text");
RETURN array::any([1,2,"text",3,NONE,3,4]);
