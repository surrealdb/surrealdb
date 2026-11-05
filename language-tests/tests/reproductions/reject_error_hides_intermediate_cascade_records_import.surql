/**
[test]
reason = """
	Setup for reject_error_hides_intermediate_cascade_records. A visible `alpha`
	the record user may delete, then two hidden hops: `beta` references alpha ON
	DELETE CASCADE, `gamma` references beta ON DELETE CASCADE, and `delta`
	references gamma ON DELETE REJECT. `beta`, `gamma` and `delta` are
	SELECT-hidden. Deleting `alpha:1` cascades through `beta:1` into `gamma:1`,
	whose delete `delta:1` rejects, so the rejection is wrapped once per hop.
"""
run = false
*/

DEFINE TABLE alpha SCHEMALESS PERMISSIONS FULL;

DEFINE TABLE beta SCHEMALESS PERMISSIONS
	FOR select, create, update, delete NONE;
DEFINE FIELD parent ON beta TYPE record<alpha> REFERENCE ON DELETE CASCADE;

DEFINE TABLE gamma SCHEMALESS PERMISSIONS
	FOR select, create, update, delete NONE;
DEFINE FIELD parent ON gamma TYPE record<beta> REFERENCE ON DELETE CASCADE;

DEFINE TABLE delta SCHEMALESS PERMISSIONS
	FOR select, create, update, delete NONE;
DEFINE FIELD target ON delta TYPE record<gamma> REFERENCE ON DELETE REJECT;

DEFINE ACCESS user ON DATABASE TYPE RECORD SIGNIN ( $rid );

CREATE user:alice;
CREATE alpha:1;
CREATE beta:1 SET parent = alpha:1;
CREATE gamma:1 SET parent = beta:1;
CREATE delta:1 SET target = gamma:1;
