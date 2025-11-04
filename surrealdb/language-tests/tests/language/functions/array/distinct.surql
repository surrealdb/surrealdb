/**
[test]

[[test.results]]
value = "[]"

[[test.results]]
error = "Incorrect arguments for function array::distinct(). Argument 1 was the wrong type. Expected `array` but found `'some text'`"

[[test.results]]
value = "[1, 2, 3, 4]"

[[test.results]]
value = "[{  }, { a: 1 }, [], [1], 'a', 'b', 1d, 1h]"

*/
array::distinct([]);
array::distinct("some text");
array::distinct([1,2,1,3,3,4]);
array::distinct([{},{},{a: 1}, [],[1], [1], "a", "a", "b", 1d, 1h, 1d]);
