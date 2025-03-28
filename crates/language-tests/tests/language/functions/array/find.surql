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
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

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
