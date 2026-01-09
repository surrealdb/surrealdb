/**
[test]

[[test.results]]
error = "Incorrect arguments for function array::len(). Expected 1 argument"

[[test.results]]
value = "0"

[[test.results]]
error = "Incorrect arguments for function array::len(). Argument 1 was the wrong type. Expected `array` but found `'some text'`"

[[test.results]]
value = "6"

*/
array::len();
array::len([]);
array::len("some text");
array::len([1,2,"text",3,3,4]);
