/**
[test]

[[test.results]]
value = "[1, 2]"

[[test.results]]
value = "[0, 2]"

[[test.results]]
value = "[]"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "An error occurred: ERROR"

[[test.results]]
value = "[0, 1, 2, 3, 4, 5]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[0, 1, 2, 3, 4, 5]"

*/

array::filter_index([5, 7, 9], |$v| $v > 6);
array::filter_index(["hello_world", "goodbye world", "hello wombat", "goodbye world"], |$v| $v CONTAINS 'hello');
array::filter_index(["nothing here"], |$v| $v == 3);
array::filter_index(["nothing here"], |$v| {
	continue;
});
array::filter_index(["nothing here"], |$v| {
	break;
});
array::filter_index([1,2,3,4,5,6], |$v| {
	if $v > 3 {
		throw "ERROR"
	}
});
array::filter_index([1,2,3,4,5,6], |$v| {
	return 1d
});
array::filter_index([1,2,3,4,5,6], |$v| {
	return ""
});
array::filter_index([1,2,3,4,5,6], |$v| {
	return []
});
array::filter_index([1,2,3,4,5,6], |$v| {
	return {a: 1}
});
