/**
[env]
imports = ["upgrade/define/3.00.00_field_element_union_with_any_import.surql"]

[test]
upgrade = true
version = ">=3.4.0-a"
reason = """
A field of kind `array | array<int>` implies `any` for its elements, since its untyped member
describes them as anything. Releases up to 3.3 derive the union `any | int` instead, and write it
as the kind of `v.*`: 3.0 to 3.2 as a structured kind, which this binary renders to text when it
reads the definition, and 3.3 as that text. The kind grammar refuses `any` as a member of a union,
so the stored text reads as the `any` it admits, which is the definition this binary writes for the
same field. The table's definitions and rows read back, and the table takes new definitions and
writes.

The read side is bounded to the current checkout: a 3.0 to 3.2 target lists the union as it
stored it, and a 3.3 target cannot read the table's definitions at all.
"""

[[test.results]]
value = "{ v: 'DEFINE FIELD v ON up_field_element_any TYPE array | array<int> PERMISSIONS FULL', \"v.*\": 'DEFINE FIELD v.* ON up_field_element_any TYPE any PERMISSIONS FULL' }"

[[test.results]]
value = "[{ id: up_field_element_any:ints, v: [1, 2] }, { id: up_field_element_any:mixed, v: ['a', 1] }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: up_field_element_any:text, v: ['b'], w: 'x' }]"

*/

-- The table's definitions read back, the one implied for the field's elements as `any`.
RETURN (INFO FOR TABLE up_field_element_any).fields;
-- The rows the old binary wrote read back unchanged.
SELECT * FROM up_field_element_any ORDER BY id;
-- The table takes a new definition, and a write.
DEFINE FIELD OVERWRITE w ON up_field_element_any TYPE option<string>;
UPSERT up_field_element_any:text SET v = ['b'], w = 'x';
