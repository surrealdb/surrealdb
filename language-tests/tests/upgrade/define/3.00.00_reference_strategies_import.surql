/**
[test]
reason = "Setup for reference_strategies.surql: the old binary stores one REFERENCE field per ON DELETE strategy, each with a live referencing row, so the new binary has real state to decode and enforce against."
run = false
*/
DEFINE TABLE up_reference_strategies_target SCHEMALESS;
DEFINE TABLE up_reference_strategies_reject SCHEMALESS;
DEFINE TABLE up_reference_strategies_ignore SCHEMALESS;
DEFINE TABLE up_reference_strategies_cascade SCHEMALESS;
DEFINE TABLE up_reference_strategies_custom SCHEMALESS;

DEFINE FIELD target ON up_reference_strategies_reject
    TYPE record<up_reference_strategies_target> REFERENCE ON DELETE REJECT;
DEFINE FIELD target ON up_reference_strategies_ignore
    TYPE record<up_reference_strategies_target> REFERENCE ON DELETE IGNORE;
DEFINE FIELD target ON up_reference_strategies_cascade
    TYPE record<up_reference_strategies_target> REFERENCE ON DELETE CASCADE;
DEFINE FIELD target ON up_reference_strategies_custom
    TYPE record<up_reference_strategies_target> REFERENCE ON DELETE THEN (UPDATE ONLY $this SET orphaned = $reference);

CREATE up_reference_strategies_target:reject;
CREATE up_reference_strategies_target:ignore;
CREATE up_reference_strategies_target:cascade;
CREATE up_reference_strategies_target:custom;

CREATE up_reference_strategies_reject:1 SET target = up_reference_strategies_target:reject;
CREATE up_reference_strategies_ignore:1 SET target = up_reference_strategies_target:ignore;
CREATE up_reference_strategies_cascade:1 SET target = up_reference_strategies_target:cascade;
CREATE up_reference_strategies_custom:1 SET target = up_reference_strategies_target:custom;
