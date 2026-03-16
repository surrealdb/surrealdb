/**
[test]
reason = "Test selecting all fields from nested array properties via graph traversal"

[[test.results]]
value = "[{ id: a:1 }, { id: a:2 }]"

[[test.results]]
value = "[{ id: edge:1, in: a:1, out: a:2 }]"

[[test.results]]
value = "[a:2]"

[[test.results]]
value = "[{ id: a:2 }]"
*/

CREATE a:1, a:2;
RELATE a:1->edge:1->a:2;
a:1->edge.out;
a:1->edge.out.*;
