/**
[test]

[[test.results]]
value = "[{ id: edge:1, in: a:1, name: 'A', out: a:2 }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "[NULL]"

[[test.results]]
value = "[[{ op: 'replace', path: '', value: { id: edge:4, in: a:1, name: 'D', out: a:2 } }]]"

[[test.results]]
value = "[{ id: edge:5, in: a:1, name: 'E', out: a:2 }]"

[[test.results]]
value = "[NONE]"

[[test.results]]
value = "[{ name: 'G' }]"

[[test.results]]
value = "[{ alias: 'H', the_id: edge:8 }]"

*/

RELATE a:1->edge:1->a:2 SET name = 'A';
RELATE a:1->edge:2->a:2 SET name = 'B' RETURN NONE;
RELATE a:1->edge:3->a:2 SET name = 'C' RETURN NULL;
RELATE a:1->edge:4->a:2 SET name = 'D' RETURN DIFF;
RELATE a:1->edge:5->a:2 SET name = 'E' RETURN AFTER;
RELATE a:1->edge:6->a:2 SET name = 'F' RETURN BEFORE;
RELATE a:1->edge:7->a:2 SET name = 'G' RETURN name;
RELATE a:1->edge:8->a:2 SET name = 'H' RETURN name AS alias, id AS the_id;
