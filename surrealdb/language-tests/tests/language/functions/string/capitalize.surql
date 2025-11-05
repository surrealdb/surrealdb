/**
[test]

[[test.results]]
value = '""'

[[test.results]]
value = '"Word"'

[[test.results]]
value = '"Word"'

[[test.results]]
value = '"A Sentence With Multiple Words"'

[[test.results]]
value = '"A Sentence With Multiple Words"'

[[test.results]]
value = '"Hello  World"'

[[test.results]]
value = '"Hello\tWorld"'

[[test.results]]
value = '"Hello\nWorld"'

[[test.results]]
value = '"Hello   Multiple   Spaces"'

[[test.results]]
value = '" Start With Space"'

[[test.results]]
value = '"End With Space "'

[[test.results]]
value = '"Mixed\t\n Whitespace\r\nTypes"'

[[test.results]]
value = '"   "'

[[test.results]]
value = '"123 Number Start"'

[[test.results]]
value = '"ALREADY UPPERCASE"'

*/
string::capitalize("");
string::capitalize("word");
string::capitalize("Word");
string::capitalize("a sentence with multiple words");
string::capitalize("a Sentence with Multiple words");
string::capitalize("hello  world");
string::capitalize("hello\tworld");
string::capitalize("hello\nworld");
string::capitalize("hello   multiple   spaces");
string::capitalize(" start with space");
string::capitalize("end with space ");
string::capitalize("mixed\t\n whitespace\r\ntypes");
string::capitalize("   ");
string::capitalize("123 number start");
string::capitalize("ALREADY UPPERCASE");
