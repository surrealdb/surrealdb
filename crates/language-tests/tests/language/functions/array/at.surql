/**
[test]

[[test.results]]
value = "'hello'"

[[test.results]]
value = "'world'"

[[test.results]]
value = "'world'"

[[test.results]]
value = "'world'"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "2"

[[test.results]]
error = "Incorrect arguments for function array::at(). Argument 2 was the wrong type. Expected `int` but found `[1, 2]`"

*/


RETURN array::at(["hello", "world"], 0);
RETURN array::at(["hello", "world"], 1f);
RETURN array::at(["hello", "world"], 1dec);
RETURN array::at(["hello", "world"], -1);
RETURN array::at(["hello", "world"], 3);
RETURN array::at(["hello", "world"], -3);

RETURN array::at([], 0);
RETURN array::at([], 3);
RETURN array::at([], -3);

let $array = [1,2,3];
array::at($array,1);
array::at($array,[1,2]);
