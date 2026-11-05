/**
[env]
namespace = true
database = true
imports = ["pgsql/_typed_columns.surql"]

[test]
reason = "Text written to a typed column, or compared with an integer key, is read as the column's type, so a client that sends every value as text works against typed tables: a text literal assigned to an integer, decimal or boolean column is cast, and a text literal compared with an integer id names the record of that integer key."

[[test.results]]
value = "[{ active: true, id: 7, price: 12.5dec, qty: 5 }]"

[[test.results]]
value = "[{ active: true, id: 7, price: 12.5dec, qty: 5 }]"

[[test.results]]
value = "[{ qty: 9 }]"

[[test.results]]
value = "[{ qty: 9 }]"
*/

INSERT INTO item (id, qty, price, active) VALUES ('7', '5', '12.50', 'true');

SELECT id, qty, price, active FROM item WHERE id = '7';

UPDATE item SET qty = '9' WHERE id = '7' RETURNING qty;

SELECT qty FROM item WHERE id IN ('7', '8');
