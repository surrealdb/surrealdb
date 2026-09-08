/**
[test]
reason = "Setup for field_flexible.surql: the old binary defines a SCHEMAFULL table carrying a FLEXIBLE object field alongside a strictly typed sibling, and stores rows whose flexible content takes several shapes."
run = false
*/
DEFINE TABLE up_field_flexible_doc SCHEMAFULL;
DEFINE FIELD label ON up_field_flexible_doc TYPE string;
DEFINE FIELD payload ON up_field_flexible_doc TYPE object FLEXIBLE;
DEFINE FIELD payload.kind ON up_field_flexible_doc TYPE string;

-- Scalars alongside the one declared sub-field.
CREATE up_field_flexible_doc:flat SET
	label = 'flat',
	payload = { kind: 'flat', count: 1, ok: true };

-- An array of undeclared objects, one of them holding an array.
CREATE up_field_flexible_doc:listy SET
	label = 'listy',
	payload = { kind: 'listy', rows: [{ n: 1, tags: ['a', 'b'] }, { n: 2, tags: [] }] };

-- Objects nested two levels below anything the schema describes.
CREATE up_field_flexible_doc:nested SET
	label = 'nested',
	payload = { kind: 'nested', outer: { inner: { deep: 'value', nums: [1, 2, 3] } } };
