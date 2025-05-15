/**
[test]

[[test.results]]
error = "Incorrect arguments for function string::replace(). Output must not exceed 1048576 bytes."

*/
string::replace(string::repeat("A",1000),/()/,string::repeat("B",10000))
