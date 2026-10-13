/**
[test]
reason = "Tests string::words splitting a string into words on whitespace"

[[test.results]]
value = "['one', 'two', 'three']"

[[test.results]]
value = "['leading', 'and', 'trailing']"

[[test.results]]
value = "['multiple', 'internal', 'spaces']"

[[test.results]]
value = "[]"

[[test.results]]
value = "['single']"

[[test.results]]
value = "['héllo', 'wörld', 'unicode']"
*/

string::words("one two three");
string::words("  leading and trailing  ");
string::words("multiple    internal     spaces");
string::words("");
string::words("single");
string::words("héllo wörld unicode");
