/**
[test]

*/


RELATE a:1->edge:1->a:2 SET name = 'A';
RELATE a:1->edge:2->a:2 SET name = 'B' RETURN NONE;
RELATE a:1->edge:3->a:2 SET name = 'C' RETURN NULL;
RELATE a:1->edge:4->a:2 SET name = 'D' RETURN DIFF;
RELATE a:1->edge:5->a:2 SET name = 'E' RETURN AFTER;
RELATE a:1->edge:6->a:2 SET name = 'F' RETURN BEFORE;
RELATE a:1->edge:7->a:2 SET name = 'G' RETURN name;
RELATE a:1->edge:8->a:2 SET name = 'H' RETURN name AS alias, id AS the_id;
