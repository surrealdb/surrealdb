/**
[test]

[[test.results]]
value = "[1, 3, 5]"

[[test.results]]
value = "['a0', 'b1', 'c2']"

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
error = "Invalid control flow statement, break or continue statement found outside of loop."

[[test.results]]
value = "[]"

*/
array::map([1,2,3], |$n, $i| $n + $i);
array::map(["a","b","c"], |$n, $i| $n + <string> $i);
array::map([1,2,3], |$n, $i|{
	continue;
});
array::map([1,2,3], |$n, $i|{
	break;
});
array::map([], |$n, $i|{
	break;
});
