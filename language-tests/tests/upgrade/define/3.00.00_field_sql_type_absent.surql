/**
[env]
imports = ["upgrade/define/3.00.00_field_sql_type_absent_import.surql"]

[test]
upgrade = true
introduced = "3.0.0"
version = ">=3.4.0-a"
reason = """
A field defined by an older binary has no declared SQL type, so the new binary
holds its writes to the field's kind alone: no length or scale is applied. A
declared SQL type added on the new binary is enforced on the writes after it."""

[[test.results]]
value = "[{ id: person:2, name: 'bartholomew', price: 2.12345dec }]"

[[test.results]]
value = "[{ id: person:1, name: 'alexander', price: 1.005dec }, { id: person:2, name: 'bartholomew', price: 2.12345dec }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "value too long for type character varying(5) (field `name` of `person:3`, SQL TYPE varchar(5))"

[[test.results]]
value = "[{ id: person:4, name: 'dan', price: 3.46dec }]"

[[test.results]]
value = "[{ id: person:1, name: 'alexander', price: 1.005dec }, { id: person:2, name: 'bartholomew', price: 2.12345dec }, { id: person:4, name: 'dan', price: 3.46dec }]"
*/
-- Without a declared SQL type only the kind applies.
CREATE person:2 SET name = 'bartholomew', price = 2.12345dec;
SELECT id, name, price FROM person ORDER BY id;
-- A declared SQL type added now applies to later writes.
DEFINE FIELD OVERWRITE name ON person TYPE string SQL TYPE POSTGRESQL 'varchar(5)';
DEFINE FIELD OVERWRITE price ON person TYPE decimal SQL TYPE POSTGRESQL 'numeric(6,2)';
CREATE person:3 SET name = 'cornelius', price = 1dec;
CREATE person:4 SET name = 'dan', price = 3.456dec;
SELECT id, name, price FROM person ORDER BY id;
