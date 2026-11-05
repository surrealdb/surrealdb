/**
[test]
reason = "Tests string::split splitting a string by a delimiter into an array"

[[test.results]]
value = "['a', 'b', 'c']"

[[test.results]]
value = "['a,b,c']"

[[test.results]]
value = "['']"

[[test.results]]
value = "['a', '', 'c']"

[[test.results]]
value = "['', 'a', '']"

[[test.results]]
value = "['one', 'two', 'three']"

[[test.results]]
value = "['', 'a', 'b', 'c', '']"

[[test.results]]
value = "['héllo w', 'rld']"
*/

string::split("a,b,c", ",");
string::split("a,b,c", ";");
string::split("", ",");
string::split("a,,c", ",");
string::split(",a,", ",");
string::split("one::two::three", "::");
string::split("abc", "");
string::split("héllo wörld", "ö");
