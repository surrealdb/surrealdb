/**
[test]
version = "<3.0.0"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

*/
["abcde","bcde","cdef"] *~ "cd";
["abcde","bcde","cdef"] *~ "g";
["abcde","bcde","cdef"] *~ "b";
