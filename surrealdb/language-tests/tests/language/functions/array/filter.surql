/**
[test]

[[test.results]]
value = "[7, 9]"

[[test.results]]
value = "['hello_world', 'hello wombat']"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "An error occurred: ERROR"

[[test.results]]
value = "[1, 2, 3, 4, 5, 6]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[1, 2, 3, 4, 5, 6]"

*/
array::filter([5, 7, 9], |$v| $v > 6);
array::filter(["hello_world", "goodbye world", "hello wombat", "goodbye world"], |$v| $v CONTAINS 'hello');
array::filter(["nothing here"], |$v| $v == 3);
array::filter(["nothing here"], |$v| {
	continue;
});
array::filter(["nothing here"], |$v| {
	break;
});
array::filter([1,2,3,4,5,6], |$v| {
	if $v > 3 {
		throw "ERROR"
	}
});
array::filter([1,2,3,4,5,6], |$v| {
	return 1d
});
array::filter([1,2,3,4,5,6], |$v| {
	return ""
});
array::filter([1,2,3,4,5,6], |$v| {
	return []
});
array::filter([1,2,3,4,5,6], |$v| {
	return {a: 1}
});
