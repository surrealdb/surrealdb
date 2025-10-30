/**
[test]

[[test.results]]
value = "[{ id: tgt:1, txt: 'Hello' }]"

[[test.results]]
value = "[{ id: tgt:2, txt: 'grouping' }]"

[[test.results]]
value = "[{ id: tgt:3, txt: 'world' }]"

[[test.results]]
value = '''[{ "array::join": 'Hello grouping world' }]'''

*/

CREATE tgt:1 SET txt = "Hello";
CREATE tgt:2 SET txt = "grouping";
CREATE tgt:3 SET txt = "world";

SELECT array::join(txt," ") FROM tgt GROUP ALL;

