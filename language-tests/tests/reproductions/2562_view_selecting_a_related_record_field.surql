/**
[env]
timeout = 5000

[test]
reason = "Pins current behaviour: a view is maintained from writes to its source table, so a change to a related record a view row selected a field of (x.letter) does not reach the view. Not changed here."
issue = 2562

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: x:a, letter: 'a' }]"

[[test.results]]
value = "[{ id: y:1, x: x:a }]"

[[test.results]]
value = "[{ id: z:1, x: { letter: 'a' } }]"

[[test.results]]
value = "[{ id: x:a, letter: 'b' }]"

[[test.results]]
value = "[{ x: { letter: 'b' } }]"

[[test.results]]
value = "[{ id: z:1, x: { letter: 'a' } }]"

*/

DEFINE TABLE y;
DEFINE TABLE z AS SELECT x.letter FROM y;
CREATE x:a SET letter = 'a';
CREATE y:1 SET x = x:a;
SELECT * FROM z;
UPDATE x:a SET letter = 'b';
SELECT x.letter FROM y;
SELECT * FROM z;
