/**
[test]

[[test.results]]
error = "Incorrect arguments for function array::last(). Expected 1 argument"

[[test.results]]
value = "'world'"

[[test.results]]
value = "10"

[[test.results]]
value = "NONE"

*/
array::last();
array::last(["hello", "world"]);
array::last([["hello", "world"], 10]);
array::last([]);
