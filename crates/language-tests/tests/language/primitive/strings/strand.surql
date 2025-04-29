/**
[test]
reason = "varies strands"

[[test.results]]
value = "'foo'"

[[test.results]]
value = "'bar'"

[[test.results]]
value = """
'with
a
newline'"""

[[test.results]]
value = "'foo'"

[[test.results]]
value = "'bar'"

[[test.results]]
value = """
'with
a
newline'"""

[[test.results]]
value = "'foo'"

[[test.results]]
value = "'s'"

[[test.results]]
value = """
'with
a
newline'"""

[[test.results]]
value = "'foo'"

[[test.results]]
value = "'s'"

[[test.results]]
value = """
'with
a
newline'"""

[[test.results]]
value = """
'
'"""

[[test.results]]
value = "'\r'"

[[test.results]]
value = "'\\\\'"

[[test.results]]
value = "'\"'"

[[test.results]]
value = "'\b'"

[[test.results]]
value = "'\f'"

[[test.results]]
value = """
'
'"""

[[test.results]]
value = "'\t'"

[[test.results]]
value = "'/'"

[[test.results]]
value = """
'
'"""

[[test.results]]
value = "'\r'"

[[test.results]]
value = "'\\\\'"

[[test.results]]
value = '''"'"'''

[[test.results]]
value = "'\b'"

[[test.results]]
value = "'\f'"

[[test.results]]
value = """
'
'"""

[[test.results]]
value = "'\t'"

[[test.results]]
value = "'/'"

*/

"foo";
"bar";

"with
a
newline";

'foo';
'bar';

'with
a
newline';

s"foo";
s"s";

s"with
a
newline";

s'foo';
s's';

s'with
a
newline';

"\n";
"\r";
"\\";
"\"";
"\b";
"\f";
"\n";
"\t";
"\/";

'\n';
'\r';
'\\';
'\'';
'\b';
'\f';
'\n';
'\t';
'\/';
