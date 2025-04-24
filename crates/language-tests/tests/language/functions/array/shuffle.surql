/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::shuffle(). Argument 1 was the wrong type. Expected `array` but found `3`"

[[test.results]]
value = "[4]"

[[test.results]]
value = "[1, 1, 1]"

[[test.results]]
# TODO: Look into why this test succeeded with `CONSTAINSALL`, invalid syntax and `$results` the wrong parameter.
match = "$result CONTAINSALL [1,2,'text',3,4]"
error = false
*/

array::shuffle([]);
array::shuffle(3);
array::shuffle([4]);
array::shuffle([1,1,1]);
array::shuffle([1,2,"text",3,3,4]); // find a way to check randomness
