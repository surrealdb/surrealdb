/**
[test]

[[test.results]]
value = "[{ id: user:tobie, name: 'Tobie' }]"

[[test.results]]
value = "[{ id: user:jaime, name: 'Jaime' }]"

[[test.results]]
value = "[{ id: product:phone, price: 1000 }]"

[[test.results]]
value = "[{ id: product:laptop, price: 3000 }]"

[[test.results]]
value = "[{ id: bought:1, in: user:tobie, out: product:phone, payment_method: 'VISA' }]"

[[test.results]]
value = "[{ id: bought:2, in: user:tobie, out: product:laptop, payment_method: 'VISA' }]"

[[test.results]]
value = "[{ id: bought:3, in: user:jaime, out: product:laptop, payment_method: 'VISA' }]"

[[test.results]]
value = "[{ id: user:jaime, name: 'Jaime', purchases: [bought:3] }, { id: user:tobie, name: 'Tobie', purchases: [bought:1, bought:2] }]"

[[test.results]]
value = "[{ id: user:jaime, name: 'Jaime', products: [{ id: product:laptop, price: 3000 }] }, { id: user:tobie, name: 'Tobie', products: [{ id: product:phone, price: 1000 }, { id: product:laptop, price: 3000 }] }]"

[[test.results]]
value = "[{ id: user:jaime, name: 'Jaime', products: [{ id: product:laptop, price: 3000 }] }, { id: user:tobie, name: 'Tobie', products: [{ id: product:phone, price: 1000 }, { id: product:laptop, price: 3000 }] }]"

[[test.results]]
value = "[{ id: user:jaime, name: 'Jaime', products: [{ id: bought:3, in: user:jaime, out: product:laptop, payment_method: 'VISA' }] }, { id: user:tobie, name: 'Tobie', products: [{ id: bought:1, in: user:tobie, out: product:phone, payment_method: 'VISA' }, { id: bought:2, in: user:tobie, out: product:laptop, payment_method: 'VISA' }] }]"

[[test.results]]
value = "[{ id: user:jaime, name: 'Jaime', purchases: [{ id: bought:3, in: user:jaime, out: { id: product:laptop, price: 3000 }, payment_method: 'VISA' }] }, { id: user:tobie, name: 'Tobie', purchases: [{ id: bought:1, in: user:tobie, out: { id: product:phone, price: 1000 }, payment_method: 'VISA' }, { id: bought:2, in: user:tobie, out: { id: product:laptop, price: 3000 }, payment_method: 'VISA' }] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: user:jaime, name: 'Jaime', purchases: [{ id: bought:3, in: user:jaime, out: { id: product:laptop, price: 3000 }, payment_method: 'VISA' }] }, { id: user:tobie, name: 'Tobie', purchases: [{ id: bought:1, in: user:tobie, out: { id: product:phone, price: 1000 }, payment_method: 'VISA' }, { id: bought:2, in: user:tobie, out: { id: product:laptop, price: 3000 }, payment_method: 'VISA' }] }]"

[[test.results]]
value = "[{ id: user:jaime, name: 'Jaime', purchases: [{ id: bought:3, in: user:jaime, out: { id: product:laptop, price: 3000 }, payment_method: 'VISA' }] }, { id: user:tobie, name: 'Tobie', purchases: [{ id: bought:1, in: user:tobie, out: { id: product:phone, price: 1000 }, payment_method: 'VISA' }, { id: bought:2, in: user:tobie, out: { id: product:laptop, price: 3000 }, payment_method: 'VISA' }] }]"

[[test.results]]
value = "[{ id: user:jaime, name: 'Jaime', purchases: [{ id: bought:3, in: user:jaime, out: { id: product:laptop, price: 3000 }, payment_method: 'VISA' }] }, { id: user:tobie, name: 'Tobie', purchases: [{ id: bought:1, in: user:tobie, out: { id: product:phone, price: 1000 }, payment_method: 'VISA' }, { id: bought:2, in: user:tobie, out: { id: product:laptop, price: 3000 }, payment_method: 'VISA' }] }]"

[[test.results]]
value = "NONE"

[[test.results]]
error = "Found 1f on FETCH CLAUSE, but FETCH expects an idiom, a string or fields"

*/
CREATE user:tobie SET name = 'Tobie';
CREATE user:jaime SET name = 'Jaime';
CREATE product:phone SET price = 1000;
CREATE product:laptop SET price = 3000;
RELATE user:tobie->bought->product:phone SET id = bought:1, payment_method = 'VISA';
RELATE user:tobie->bought->product:laptop SET id = bought:2, payment_method = 'VISA';
RELATE user:jaime->bought->product:laptop SET id = bought:3, payment_method = 'VISA';
SELECT *, ->bought AS purchases FROM user;
SELECT *, ->bought.out.* AS products FROM user;
SELECT *, ->bought->product.* AS products FROM user;
SELECT *, ->bought AS products FROM user FETCH products;
SELECT *, ->(bought AS purchases) FROM user FETCH purchases, purchases.out;
LET $param1 = 'purchases';
SELECT *, ->(bought AS purchases) FROM user FETCH $param1, purchases.out;
SELECT *, ->(bought AS purchases) FROM user FETCH type::field('purchases'), purchases.out;
SELECT *, ->(bought AS purchases) FROM user FETCH type::fields([$param1, 'purchases.out']);
LET $faultyparam = 1.0f;
SELECT *, ->(bought AS purchases) FROM user FETCH $faultyparam, purchases.out;
