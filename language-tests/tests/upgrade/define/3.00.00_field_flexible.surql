/**
[env]
imports = ["upgrade/define/3.00.00_field_flexible_import.surql"]

[test]
upgrade = true
reason = """
Covers the FLEXIBLE flag on a stored FieldDefinition and the arbitrary
undeclared content it lets a SCHEMAFULL table hold.

The new binary must decode the flag, return every stored shape byte-identically,
accept a further undeclared shape of its own, and still enforce the declared
types around it — FLEXIBLE relaxes only the keys the schema does not describe.
"""

[[test.results]]
match = '''
    $result.fields.payload == 'DEFINE FIELD payload ON up_field_flexible_doc TYPE object FLEXIBLE PERMISSIONS FULL'
        && $result.fields.label == 'DEFINE FIELD label ON up_field_flexible_doc TYPE string PERMISSIONS FULL'
        && $result.fields['payload.kind'] == 'DEFINE FIELD payload.kind ON up_field_flexible_doc TYPE string PERMISSIONS FULL'
'''

[[test.results]]
value = '''[
	{ id: up_field_flexible_doc:flat, label: 'flat', payload: { count: 1, kind: 'flat', ok: true } },
	{ id: up_field_flexible_doc:listy, label: 'listy', payload: { kind: 'listy', rows: [{ n: 1, tags: ['a', 'b'] }, { n: 2, tags: [] }] } },
	{ id: up_field_flexible_doc:nested, label: 'nested', payload: { kind: 'nested', outer: { inner: { deep: 'value', nums: [1, 2, 3] } } } }
]'''

[[test.results]]
value = '''[
	{ id: up_field_flexible_doc:after, label: 'after', payload: { kind: 'after', matrix: [[1, 2], [3, 4]], meta: { by: 'new binary', deeper: { and: { deeper: 'still' } } } } }
]'''

[[test.results]]
error = true

[[test.results]]
error = true

[[test.results]]
value = "[up_field_flexible_doc:after, up_field_flexible_doc:flat, up_field_flexible_doc:listy, up_field_flexible_doc:nested]"

*/
INFO FOR TABLE up_field_flexible_doc;
-- Every shape the old binary stored reads back unchanged.
SELECT * FROM up_field_flexible_doc ORDER BY id;
-- The new binary writes a shape the schema never described into the same field.
CREATE up_field_flexible_doc:after SET
	label = 'after',
	payload = { kind: 'after', matrix: [[1, 2], [3, 4]], meta: { by: 'new binary', deeper: { and: { deeper: 'still' } } } };
-- The strictly typed sibling still rejects a value of the wrong type.
CREATE up_field_flexible_doc:bad SET label = [1, 2, 3], payload = { kind: 'bad' };
-- So does the declared sub-field inside the flexible object.
CREATE up_field_flexible_doc:bad SET label = 'bad', payload = { kind: 42 };
-- Neither rejected write left a record behind.
SELECT VALUE id FROM up_field_flexible_doc ORDER BY id;
