/**
[test]

[[test.results]]
value = "[{ id: t:1, tags: ['a', 'b'] }]"

[[test.results]]
value = "[{ id: t:1, tags: ['a', 'b'] }]"

*/
CREATE t:1 SET tags = ['a', 'b'];
SELECT * FROM t SPLIT unknown_field;
