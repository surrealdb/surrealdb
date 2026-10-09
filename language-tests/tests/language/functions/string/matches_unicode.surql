/**
[test]
reason = "Regex classes, word boundaries and case folding are Unicode-aware in the default build, which carries the `regex-unicode` feature."

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "true"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

*/

string::matches("٣", /^\d$/);
string::matches("é", /^\w$/);
string::matches("\u{a0}", /^\s$/);
string::matches("café bar", /café\b/);
string::matches("ÉCOLE", /(?i)école/);
string::matches("αβγ", /^\p{Greek}+$/);
string::matches("é", /(?-u:^\w$)/);
string::matches("\u{212a}", /(?i)k/);
