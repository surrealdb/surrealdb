/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro"]

[test]
reason = "Bare field paths in a subquery FROM clause should resolve against the outer document when traversing record links"
issue = 7156

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ direct_arr_works: ['x', 'y'], files: ['a.jpg', 'b.jpg'], id: item:i1 }]"

[[test.results]]
value = "[{ files: ['a.jpg', 'b.jpg'], id: item:i1 }]"

[[test.results]]
value = "[{ files: ['a.jpg', 'b.jpg'], id: item:i1 }]"

*/

{
  DEFINE TABLE OVERWRITE shared_data SCHEMAFULL TYPE NORMAL;
  DEFINE FIELD OVERWRITE files ON shared_data TYPE option<array<object>>;
  DEFINE FIELD OVERWRITE files.*.id ON shared_data TYPE int;
  DEFINE FIELD OVERWRITE files.*.path ON shared_data TYPE string;

  DEFINE TABLE OVERWRITE item SCHEMAFULL TYPE NORMAL;
  DEFINE FIELD OVERWRITE data ON item TYPE record<shared_data>;
  DEFINE FIELD OVERWRITE files_index ON item TYPE option<array<int>>;
  DEFINE FIELD OVERWRITE direct_arr ON item TYPE option<array<string>>;

  CREATE shared_data:sd1 SET
    files = [{id: 0, path: "a.jpg"}, {id: 1, path: "b.jpg"}];

  CREATE item:i1 SET
    data = shared_data:sd1,
    files_index = [0, 1],
    direct_arr = ["x", "y"];

  RETURN "OK";
};

-- Core reproduction: bare field path `data.files` in subquery FROM traversing a record link,
-- combined with $parent.files_index in WHERE
SELECT
  id,
  (SELECT VALUE path FROM data.files WHERE id IN $parent.files_index) as files,
  (SELECT VALUE $this FROM $parent.direct_arr) as direct_arr_works
FROM item;

-- Variant: $this.data.files (explicit correlated reference) should also work
SELECT
  id,
  (SELECT VALUE path FROM $this.data.files WHERE id IN $parent.files_index) as files
FROM item;

-- Variant: $parent.data.files in FROM
SELECT
  id,
  (SELECT VALUE path FROM $parent.data.files WHERE id IN $parent.files_index) as files
FROM item;
