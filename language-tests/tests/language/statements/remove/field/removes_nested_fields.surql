/**
[env]
namespace = true
database = true

[test]
reason = "REMOVE FIELD removes every field defined in the field it removes, the element definitions its kind implied among them"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['cart', 'cart.items', 'cart.items.*', 'cart.items.*.sku', 'cart.total', 'other']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['cart', 'cart.total', 'other']"

[[test.results]]
value = "NONE"

[[test.results]]
value = "['other']"

[[test.results]]
error = "Cannot define field `cart.total`: the field `cart` it is nested in is not defined. Define `cart` first"
*/

DEFINE TABLE shop SCHEMAFULL;
DEFINE FIELD cart ON shop TYPE object;
DEFINE FIELD cart.items ON shop TYPE array<object>;
DEFINE FIELD cart.items[*].sku ON shop TYPE string;
DEFINE FIELD cart.total ON shop TYPE number;
DEFINE FIELD other ON shop TYPE string;
object::keys((INFO FOR TABLE shop).fields);
REMOVE FIELD cart.items ON shop;
object::keys((INFO FOR TABLE shop).fields);
REMOVE FIELD cart ON shop;
object::keys((INFO FOR TABLE shop).fields);
-- Nothing is left defined in `cart`, so a field in it needs `cart` again.
DEFINE FIELD cart.total ON shop TYPE number;
