/**
[test]

[[test.results]]
error = "Incorrect arguments for function array::join(). Expected 2 arguments"

[[test.results]]
error = "Incorrect arguments for function array::join(). Expected 2 arguments"

[[test.results]]
value = "''"

[[test.results]]
value = "'hello, world'"

[[test.results]]
value = "'again and again and again'"

[[test.results]]
value = "'42 and true and 1.61'"

[[test.results]]
value = "'[] and { a: 1 } and 0ns'"

*/
array::join();
array::join([]);
array::join([], "");
array::join(["hello", "world"], ", ");
array::join(["again", "again", "again"], " and ");
array::join([42, true, "1.61"], " and ");
array::join([[], {a: 1}, 0d], " and ");
