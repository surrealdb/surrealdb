/**
[test]

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "1"

[[test.results]]
value = "NONE"

[[test.results]]
value = "0"

[[test.results]]
error = "Incorrect arguments for function array::index_of(). Expected 2 arguments"
*/
array::index_of([5, 7, 9], 7);
array::index_of([5, 7, 9], 8);
array::index_of([5, 7, 9], |$v| $v >= 6);
array::index_of([5, 7, 9], |$v| $v > 100);
array::index_of(["a", "b", "a"], "a");
array::index_of([1, 2, 3]);
