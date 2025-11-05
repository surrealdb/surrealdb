/**
[test]

[[test.results]]
error = "Incorrect arguments for function array::pop(). Expected 1 argument"

[[test.results]]
value = "'world'"

[[test.results]]
value = "10"

[[test.results]]
value = "NONE"

*/
array::pop();
array::pop(["hello", "world"]);
array::pop([["hello", "world"], 10]);
array::pop([]);
