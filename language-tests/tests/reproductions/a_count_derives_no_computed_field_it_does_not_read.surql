/**
[env]
imports = ["reproductions/a_count_derives_no_computed_field_it_does_not_read_import.surql"]
auth = { namespace = "test", database = "test", access = "user", rid = "account:alice" }

[test]
reason = "A record user's aggregate over rows read one by one derives only the computed fields its own key and field regions, and the steps before it, read. A count reads no field of the row, so it derives none: the computed field on `person` throws when derived, and no aggregate over `person` reaches it. An aggregate that does read a computed field still derives it, as `item` shows."

[[test.results]]
value = "[{ count: 3 }]"

[[test.results]]
value = "[{ count: 2 }]"

[[test.results]]
value = "[{ total: 60 }]"

[[test.results]]
value = "[{ age: 10, n: 1 }, { age: 20, n: 1 }, { age: 30, n: 1 }]"

[[test.results]]
error = true

[[test.results]]
value = "[{ total: 120 }]"

[[test.results]]
value = "[{ count: 2 }]"

*/

SELECT count() FROM person GROUP ALL;
SELECT count() FROM person WHERE age > 15 GROUP ALL;
SELECT math::sum(age) AS total FROM person GROUP ALL;
SELECT age, count() AS n FROM person GROUP BY age ORDER BY age;

-- The control: a read of the computed field does derive it, so the field is the one that throws.
SELECT boom FROM person;

-- An aggregate, or a filter before one, that reads a computed field derives it.
SELECT math::sum(doubled) AS total FROM item GROUP ALL;
SELECT count() FROM item WHERE doubled > 30 GROUP ALL;
