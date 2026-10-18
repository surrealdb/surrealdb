/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
SELECT FROM an array of record ids reads each record (a missing id yields no row), so $this is the record, not the id; `id` and array methods keep the ids. Same contract as 7562.
"""
issue = 7339

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: ra:existing, name: 'Existing' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "{ mapped_values: [ra:existing], raw_ids: [ra:existing], selected_all: [{ id: ra:existing, name: 'Existing' }], selected_rows: [{ item: { id: ra:existing, name: 'Existing' } }], selected_values: [{ id: ra:existing, name: 'Existing' }] }"

[[test.results]]
value = "{ mapped_projection: [{ id: ra:existing }], selected_projection: [{ id: { id: ra:existing, name: 'Existing' } }], selected_projection_using_id_field: [{ id: ra:existing }] }"
*/
DEFINE TABLE ra SCHEMALESS;
CREATE ra:existing SET name = "Existing";
LET $ids = [ra:existing];
RETURN {
    raw_ids: $ids,
    selected_values: (SELECT VALUE $this FROM $ids),
    selected_rows: (SELECT $this AS item FROM $ids),
    selected_all: (SELECT * FROM $ids),
    mapped_values: array::map($ids, |$id| $id)
};
RETURN {
    selected_projection: (SELECT VALUE { id: $this } FROM $ids),
    selected_projection_using_id_field: (SELECT VALUE { id: $this.id } FROM $ids),
    mapped_projection: array::map($ids, |$id| { id: $id })
};
