/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "7"

[[test.results]]
value = "'hello world'"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Continue statement has been reached"

[[test.results]]
error = "Break statement has been reached"

*/
array::find([], |$v| $v >= 6);
array::find([5, 7, 9], |$v| $v >= 6);
array::find(["hello world", null, true], |$v| $v != NULL);
array::find([0, 1, 2], |$v| $v > 5);
array::find([0, 1, 2], |$v|{
	continue;
});
array::find([0, 1, 2], |$v|{
	break;
});
