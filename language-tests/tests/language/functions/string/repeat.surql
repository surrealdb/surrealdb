/**
[test]

[[test.results]]
value = '"AAA"'

[[test.results]]
value = '""'

[[test.results]]
value = '""'

[[test.results]]
value = '"abcabcabc"'

[[test.results]]
error = 'Failed to compute: "string::repeat(-1)", as the operation results in a negative value.'

*/
string::repeat("A", 3);
string::repeat("A", 0);
string::repeat("", 5);
string::repeat("abc", 3);
string::repeat("A", -1);
