/**

[test]
wip = true

*/

DEFINE TABLE product;
DEFINE FIELD subproducts ON product VALUE COMPUTED { ->contains->product };
CREATE product:one, product:two;
RELATE product:one->contains:test->product:two;
SELECT * FROM product;
UPDATE product;
SELECT * FROM product;
