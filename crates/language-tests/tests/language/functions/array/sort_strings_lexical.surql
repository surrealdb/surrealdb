/**
[test]

[[test.results]]
value = "['1', '11', '2', 'Zoo', 'Ábaco']"

[[test.results]]
value = "['Ábaco', 'Zoo', '2', '11', '1']"

[[test.results]]
value = "['1', '2', '11', 'Zoo', 'Ábaco']"

[[test.results]]
value = "['Ábaco', 'Zoo', '11', '2', '1']"

[[test.results]]
value = "['1', '11', '2', 'Ábaco', 'Zoo']"

[[test.results]]
value = "['Zoo', 'Ábaco', '2', '11', '1']"

[[test.results]]
value = "['1', '2', '11', 'Ábaco', 'Zoo']"

[[test.results]]
value = "['Zoo', 'Ábaco', '11', '2', '1']"

*/
['Zoo', 'Ábaco', '1', '2', '11'].sort();
['Zoo', 'Ábaco', '1', '2', '11'].sort("desc");
['Zoo', 'Ábaco', '1', '2', '11'].sort_natural();
['Zoo', 'Ábaco', '1', '2', '11'].sort_natural("desc");
['Zoo', 'Ábaco', '1', '2', '11'].sort_lexical();
['Zoo', 'Ábaco', '1', '2', '11'].sort_lexical("desc");
['Zoo', 'Ábaco', '1', '2', '11'].sort_natural_lexical();
['Zoo', 'Ábaco', '1', '2', '11'].sort_natural_lexical("desc");
