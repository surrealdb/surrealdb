/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

*/
array::find_index([], |$v| $v >= 6);
array::find_index([5, 7, 9], |$v| $v >= 6);
array::find_index(["hello world", null, true], |$v| $v != NULL);
array::find_index([0, 1, 2], |$v| $v > 5);
array::find_index([0, 1, 2], |$v|{
	continue;
});
array::find_index([0, 1, 2], |$v|{
	break;
});
