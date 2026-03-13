/**
[test]
run = false
*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
    SIGNIN ( SELECT * FROM user WHERE id = $id );

CREATE user:test;

DEFINE TABLE product PERMISSIONS FULL;
CREATE product:test SET name = "test product";

DEFINE FUNCTION fn::get_products() {
    SELECT * FROM product;
} PERMISSIONS NONE;
