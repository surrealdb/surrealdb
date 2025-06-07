/**
[test]
version = "<3.0.0"

[[test.results]]
value = "false"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

*/
"abcde" !~ "abcde";
"abcde" !~ "abc";
"abcde" !~ "abcdef";
"abcde" !~ "b";

