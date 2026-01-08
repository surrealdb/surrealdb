/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3, 5]"

[[test.results]]
value = "[1, 2, 3]"

[[test.results]]
value = "[1, 2, 3, 4]"

*/
array::remove([3], 0);
array::remove([3], 2);
array::remove([3,4,5], 1);
array::remove([1,2,3,4], -1);
array::remove([1,2,3,4], -10);
