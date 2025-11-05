/**
[test]

[[test.results]]
error = "Incorrect arguments for function array::insert(). Expected 2 to 3 arguments"

[[test.results]]
error = "Incorrect arguments for function array::insert(). Expected 2 to 3 arguments"

[[test.results]]
value = "[1]"

[[test.results]]
value = "[1, 1]"

[[test.results]]
value = "[3]"

[[test.results]]
value = "[3, 1]"

[[test.results]]
value = "[1, 2, 3, 5, 4]"

[[test.results]]
value = "[1, 2, 3, 4]"

[[test.results]]
value = "[1, 2, 3, 4]"

*/
array::insert();
array::insert([]);
array::insert([], 1);
array::insert([1], 1);
array::insert([3], 1, 5);
array::insert([3], 1, 1);
array::insert([1,2,3,4], 5, -1);
array::insert([1,2,3,4], 5, -9999999);
array::insert([1,2,3,4], 5, 9999999);
