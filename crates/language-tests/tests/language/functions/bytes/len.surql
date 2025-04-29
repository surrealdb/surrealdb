/**
[test]

[[test.results]]
value = "0"

[[test.results]]
error = "Incorrect arguments for function bytes::len(). Argument 1 was the wrong type. Expected `bytes` but found `true`"

[[test.results]]
error = "Incorrect arguments for function bytes::len(). Argument 1 was the wrong type. Expected `bytes` but found `[]`"

[[test.results]]
value = "2"

[[test.results]]
value = "4"

*/
bytes::len(<bytes>"");
bytes::len(true);
bytes::len([]);
bytes::len(<bytes>"π");
bytes::len(<bytes>"ππ");
