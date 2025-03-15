/**
[test]

[[test.results]]
value = "[1, 3, 5]"

[[test.results]]
value = "['a0', 'b1', 'c2']"

[[test.results]]
error = "Continue statement has been reached"

[[test.results]]
error = "Break statement has been reached"

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
