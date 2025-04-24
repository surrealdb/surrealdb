/**
[test]

[[test.results]]
value = "'hello'"

[[test.results]]
value = "['hello', 'world']"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Incorrect arguments for function array::first(). Argument 1 was the wrong type. Expected `array` but found `1`"

[[test.results]]
error = "Incorrect arguments for function array::first(). Argument 1 was the wrong type. Expected `array` but found `{ a: 1 }`"

*/
array::first(["hello", "world"]);
array::first([["hello", "world"], 10]);
array::first([]);
array::first(1);
array::first({a: 1});
