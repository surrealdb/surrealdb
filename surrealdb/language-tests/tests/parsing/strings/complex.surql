/**
[test]

[[test.results]]
value = "'String with no complex characters'"

[[test.results]]
value = "'String with some \"double quoted\" characters'"

[[test.results]]
value = '''"String with some 'escaped single quoted' characters"'''

[[test.results]]
value = "'String with some \"escaped double quoted\" characters'"

[[test.results]]
value = '''"String with some 'single' and \"double\" quoted characters"'''

*/

RETURN 'String with no complex characters';
RETURN 'String with some "double quoted" characters';
RETURN 'String with some \'escaped single quoted\' characters';
RETURN "String with some \"escaped double quoted\" characters";
RETURN "String with some 'single' and \"double\" quoted characters";
