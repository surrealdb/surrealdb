/**

[test]

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: node:a, label: 'alpha', next: node:b }]"

[[test.results]]
value = "[{ id: node:b, label: 'beta', next: node:c }]"

[[test.results]]
value = "[{ id: node:c, label: 'gamma', next: node:a, tag: 'alpha' }]"

*/

DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNUP ( CREATE type::record('node', $id) SET label = $label )
	SIGNIN ( SELECT * FROM type::record('node', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- Conditional SELECT permission that traverses a link and reads a computed field.
-- `next.tag` forces a fetch of the linked record and then accesses the computed
-- `tag` field.  `tag` itself dereferences yet another RecordId (`next.label`),
-- so the permission evaluator must keep skip_fetch_perms=true through the
-- computed-field evaluation to avoid reentrant permission checks.
DEFINE TABLE OVERWRITE node SCHEMAFULL
	PERMISSIONS
		FOR select WHERE next.tag != NONE
		FOR create, update, delete FULL;

DEFINE FIELD label ON node TYPE string;
DEFINE FIELD next ON node TYPE option<record<node>>;
DEFINE FIELD tag ON node VALUE next.label;

CREATE node:a SET label = "alpha", next = node:b;
CREATE node:b SET label = "beta", next = node:c;
CREATE node:c SET label = "gamma", next = node:a;
