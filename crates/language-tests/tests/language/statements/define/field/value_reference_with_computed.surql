/**
[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: product:one, subproducts: [] }, { id: product:two, subproducts: [] }]"

[[test.results]]
value = "[{ id: contains:test, in: product:one, out: product:two }]"

[[test.results]]
value = "[{ id: product:one, subproducts: [product:two] }, { id: product:two, subproducts: [] }]"

[[test.results]]
value = "[{ id: product:one, subproducts: [product:two] }, { id: product:two, subproducts: [] }]"

[[test.results]]
value = "[{ id: product:one, subproducts: [product:two] }, { id: product:two, subproducts: [] }]"

*/

DEFINE TABLE product;
DEFINE FIELD subproducts ON product COMPUTED ->contains->product;
CREATE product:one, product:two;
RELATE product:one->contains:test->product:two;
SELECT * FROM product;
UPDATE product;
SELECT * FROM product;
