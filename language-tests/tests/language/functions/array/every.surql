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
value = "true"

[[test.results]]
value = "false"

[[test.results]]
error = "Incorrect arguments for function array::every(). Argument 1 was the wrong type. Expected `array` but found `'some text'`"
*/
array::every([1, 2, 3], |$v| $v > 0);
array::every([1, 2, 3], |$v| $v > 2);
array::every(["foo", "foo"], "foo");
array::every(["foo", "bar"], "foo");
array::every([1, true, "yes"]);
array::every([1, 0, 2]);
array::every("some text", |$v| $v > 0);
