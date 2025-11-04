/**
[test]

[[test.results]]
value = "'the'"

[[test.results]]
value = "'th'"

[[test.results]]
value = "'th'"

[[test.results]]
value = "'the quick brown fox jumps over the lazy dog.'"

[[test.results]]
value = "'fox jumps over the lazy dog.'"

[[test.results]]
value = "'the quick brown fox jumps over the lazy dog.'"

[[test.results]]
value = "'the quick brown fox jumps over the lazy dog'"

*/
string::slice("the",0,3);
string::slice("the",0,2);
string::slice("the",0,-1);
string::slice("the quick brown fox jumps over the lazy dog.");
string::slice("the quick brown fox jumps over the lazy dog.", 16);
string::slice("the quick brown fox jumps over the lazy dog.", 0, 60);
string::slice("the quick brown fox jumps over the lazy dog.", 0, -1);
-- string::slice("the quick brown fox jumps over the lazy dog.", 16, -1);
-- string::slice("the quick brown fox jumps over the lazy dog.", -9, -1);
-- string::slice("the quick brown fox jumps over the lazy dog.", -100, -100);
