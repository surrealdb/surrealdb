/**
[test]
reason = "This test proves that $parent is fixed at the time when the query is executed, and does not change throughout the various assignments"

[[test.results]]
value = "[{ a: parent:a, b: parent:b, c: parent:c, id: test:1 }]"

[[test.results]]
value = "[{ id: parent:a }, { id: parent:b }, { id: parent:c }]"

*/

UPSERT test:1 SET 
    a = (UPSERT ONLY parent:a SET parent = $parent).id, 
    b = (UPSERT ONLY parent:b SET parent = $parent).id, 
    c = (UPSERT ONLY parent:c SET parent = $parent).id;

SELECT * FROM parent;