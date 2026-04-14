/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["compute-only"]

[test]
reason = "CASCADE DELETE on RELATION tables should clean UNIQUE index entries"
issue = 7061

# -- Setup
[[test.results]]
value = "NONE"

# -- Delete child:one (cascade should remove edge AND its unique index entry)
[[test.results]]
value = "NONE"

# -- Relation table should be empty
[[test.results]]
value = "[]"

# -- Re-create child and RELATE again: should succeed (no ghost index entry)
[[test.results]]
value = "NONE"
*/

-- Setup
{
  DEFINE TABLE OVERWRITE parent SCHEMAFULL;
  DEFINE FIELD OVERWRITE name ON parent TYPE string;

  DEFINE TABLE OVERWRITE child SCHEMAFULL;
  DEFINE FIELD OVERWRITE value ON child TYPE string;

  DEFINE TABLE OVERWRITE parent_child SCHEMAFULL TYPE RELATION IN parent OUT child;
  DEFINE FIELD OVERWRITE created ON parent_child TYPE datetime DEFAULT d'2012-01-01T00:00:00Z';
  DEFINE INDEX OVERWRITE parent_child_unique ON parent_child FIELDS in UNIQUE;

  CREATE parent:one SET name = 'Alice';
  CREATE child:one SET value = 'Secret';
  RELATE parent:one->parent_child:test->child:one;
  RETURN NONE;
};

-- Delete the child record (cascade should remove edge AND its unique index entry)
{
  DELETE child:one;
  RETURN NONE;
};

-- Relation table should be empty
SELECT * FROM parent_child;

-- Re-create child and RELATE with the same parent: should succeed
{
  CREATE child:two SET value = 'New Secret';
  RELATE parent:one->parent_child:test2->child:two;
  RETURN NONE;
};
