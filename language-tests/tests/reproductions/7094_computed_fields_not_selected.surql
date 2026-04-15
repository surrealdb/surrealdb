/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Computed fields should not be eagerly evaluated when they are not selected (issue #7094)"
issue = 7094

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false

[[test.results]]
error = false
*/

-- -----------------------------------------------------------------------------
-- Case 1: Unselected computed field with THROW() must not execute
-- -----------------------------------------------------------------------------
REMOVE TABLE IF EXISTS test_table_7094;
DEFINE TABLE OVERWRITE test_table_7094 SCHEMAFULL TYPE NORMAL;
DEFINE FIELD OVERWRITE should_error ON TABLE test_table_7094 TYPE bool;
DEFINE FIELD OVERWRITE ignore ON TABLE test_table_7094 COMPUTED {
	IF should_error {
		THROW("ignore got computed")
	};
	should_error
} TYPE bool;
CREATE test_table_7094:0 SET should_error = true RETURN NONE;
SELECT should_error FROM ONLY test_table_7094:0;

-- -----------------------------------------------------------------------------
-- Case 2: Graph traversal should not fail due to unrelated computed field on edge
-- -----------------------------------------------------------------------------
REMOVE TABLE IF EXISTS in_7094;
REMOVE TABLE IF EXISTS out_7094;
REMOVE TABLE IF EXISTS relation_7094;

DEFINE TABLE OVERWRITE in_7094 SCHEMAFULL TYPE NORMAL;
DEFINE TABLE OVERWRITE out_7094 SCHEMAFULL TYPE NORMAL;
DEFINE TABLE OVERWRITE relation_7094 SCHEMAFULL TYPE RELATION IN in_7094 OUT out_7094;

DEFINE FIELD OVERWRITE dependency ON TABLE relation_7094 DEFAULT true TYPE bool;
DEFINE FIELD OVERWRITE ignore ON TABLE relation_7094 TYPE bool COMPUTED dependency;

CREATE in_7094:0 RETURN NONE;
CREATE out_7094:0 RETURN NONE;
RELATE in_7094:0->relation_7094:0->out_7094:0 RETURN NONE;

-- This should not error because `ignore` is not selected directly.
SELECT ->relation_7094->out_7094 FROM ONLY in_7094:0;

-- -----------------------------------------------------------------------------
-- Case 3: Self-referential computed field should not recurse when unselected
-- -----------------------------------------------------------------------------
REMOVE TABLE IF EXISTS self_7094;
DEFINE TABLE OVERWRITE self_7094 SCHEMAFULL TYPE NORMAL;
DEFINE FIELD OVERWRITE break_early ON TABLE self_7094 TYPE bool;
DEFINE FIELD OVERWRITE returned_early ON TABLE self_7094 COMPUTED {
	IF break_early {
		RETURN true
	};
	(SELECT VALUE break_early FROM ONLY self_7094 LIMIT 1) = true
} TYPE bool;

CREATE self_7094:0 SET break_early = true RETURN NONE;
UPDATE self_7094:0 SET break_early = false RETURN BEFORE;

-- Selecting only `break_early` should not trigger `returned_early` recursion.
SELECT break_early FROM ONLY self_7094:0;
