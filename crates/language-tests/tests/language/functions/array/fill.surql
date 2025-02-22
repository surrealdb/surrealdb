/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
value = "[[10], [10]]"

[[test.results]]
value = "[10, 10, 10, 10, 10]"

[[test.results]]
value = "[10, 10, 10, 10, 10]"

[[test.results]]
value = "[1, 10, 10, 10, 10]"

[[test.results]]
value = "[1, 10, 3, 4, 5]"

[[test.results]]
value = "[1, 2, 3, 4, 5]"

[[test.results]]
value = "[1, 2, 3, 4, 5]"

[[test.results]]
value = "[1, 2, 3, 4, 5]"

[[test.results]]
value = "[1, 2, 10, 4, 5]"

[[test.results]]
value = "[1, 2, 10, 10, 10]"

[[test.results]]
value = "[10, 10, 10, 10, 10]"

*/
array::fill([], 10);
array::fill([1,2], [10]);
array::fill([1,2,3,4,5], 10);
array::fill([1,2,3,4,5], 10, 0, 7);
array::fill([1,NONE,NONE,NONE,NONE], 10, 1);
array::fill([1,NONE,3,4,5], 10, 1, 2);
array::fill([1,2,3,4,5], 10, 1, 1);
array::fill([1,2,3,4,5], 10, 7, 7);
array::fill([1,2,3,4,5], 10, 7, 9);
array::fill([1,2,NONE,4,5], 10, -3, -2);
array::fill([1,2,3,4,5], 10, 2, 99999999);
array::fill([1,2,3,4,5], 10, -99999999, 99999999);
