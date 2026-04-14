/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["compute-only"]

[test]
reason = "Deleting IN/OUT records before RELATION should not leave phantom UNIQUE index entries"
issue = 7132

# -- Setup + first RELATE
[[test.results]]
value = "NONE"

# -- Delete IN/OUT before relation (cascade should clean unique index)
[[test.results]]
value = "NONE"

# -- Relation table should be empty
[[test.results]]
value = "[]"

# -- Re-create and re-RELATE: should succeed (no phantom index entry)
[[test.results]]
value = "NONE"

# -- Verify relation exists
[[test.results]]
value = "[{ id: speak:test2, in: user:alice, is_default: true, out: locale:en }]"
*/

-- Setup
{
  DEFINE TABLE OVERWRITE user SCHEMAFULL;
  DEFINE FIELD OVERWRITE name ON user TYPE string;

  DEFINE TABLE OVERWRITE locale SCHEMAFULL;
  DEFINE FIELD OVERWRITE code ON locale TYPE string;

  DEFINE TABLE OVERWRITE speak SCHEMAFULL TYPE RELATION IN user OUT locale;
  DEFINE FIELD OVERWRITE is_default ON speak TYPE option<bool>;
  DEFINE INDEX OVERWRITE speak_unique ON speak FIELDS in, out UNIQUE;

  CREATE user:alice SET name = 'alice';
  CREATE locale:en SET code = 'en';
  RELATE user:alice->speak:test1->locale:en SET is_default = true;
  RETURN NONE;
};

-- Delete IN/OUT records BEFORE the relation (problematic order)
{
  DELETE user;
  DELETE locale;
  DELETE speak;
  RETURN NONE;
};

-- Relation table should be empty
SELECT * FROM speak;

-- Re-create and re-RELATE with same values: should succeed
{
  CREATE user:alice SET name = 'alice';
  CREATE locale:en SET code = 'en';
  RELATE user:alice->speak:test2->locale:en SET is_default = true;
  RETURN NONE;
};

-- Verify the new relation exists
SELECT * FROM speak;
