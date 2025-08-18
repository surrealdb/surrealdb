/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: product:1, subproducts: [] }, { id: product:2, subproducts: [] }, { id: product:3, subproducts: [] }]"

[[test.results]]
value = "[{ id: contains:1, in: product:1, out: product:2 }]"

[[test.results]]
value = "[{ id: contains:2, in: product:2, out: product:3 }]"

[[test.results]]
value = "[{ id: product:1, subproducts: [{ id: product:2, subproducts: [{ id: product:3, subproducts: [] }] }] }, { id: product:2, subproducts: [{ id: product:3, subproducts: [] }] }, { id: product:3, subproducts: [] }]"

[[test.results]]
value = "[{ id: product:3, subproducts: [] }]"

[[test.results]]
value = "[{ id: product:1, subproducts: [{ id: product:2, subproducts: [{ id: product:3, subproducts: [] }] }] }, { id: product:2, subproducts: [{ id: product:3, subproducts: [] }] }]"

*/
DEFINE TABLE product;
DEFINE FIELD subproducts ON product COMPUTED ->contains->product.*;
CREATE product:1, product:2, product:3;
RELATE product:1->contains:1->product:2;
RELATE product:2->contains:2->product:3;
SELECT * FROM product;
SELECT * FROM product WHERE subproducts = [];
SELECT * FROM product WHERE subproducts.len() = 1;
