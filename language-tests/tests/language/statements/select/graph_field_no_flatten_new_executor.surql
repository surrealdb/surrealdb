/**
[test]
reason = "Test that SELECT VALUE ->edge.field does not flatten nested arrays (new executor)"
*/

CREATE a:1, a:2;
RELATE a:1->b:1->a:2 SET list = [1, 2, 3];
RELATE a:1->b:2->a:2 SET list = [4, 5, 6];
SELECT VALUE ->b.list FROM a:1;
