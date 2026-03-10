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
value = "[{ id: node:a, label: 'alpha', next: node:b }]"

[[test.results]]
value = "[{ id: node:b, label: 'beta', next: node:c }]"

[[test.results]]
value = "[{ id: node:c, label: 'gamma', next: node:a }]"

*/
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNUP ( CREATE type::record('node', $id) SET label = $label )
	SIGNIN ( SELECT * FROM type::record('node', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

-- Conditional SELECT permission uses a subquery that scans the node table.
-- The inner scan triggers the same conditional permission on each inner record,
-- which would recurse infinitely on the cyclic next links without
-- skip_fetch_perms propagation.
DEFINE TABLE OVERWRITE node SCHEMAFULL
	PERMISSIONS
		FOR select WHERE (SELECT VALUE label FROM node WHERE id = $this.next LIMIT 1) != NONE
		FOR create, update, delete FULL;

DEFINE FIELD label ON node TYPE string;
DEFINE FIELD next ON node TYPE option<record<node>>;

CREATE node:a SET label = "alpha", next = node:b;
CREATE node:b SET label = "beta", next = node:c;
CREATE node:c SET label = "gamma", next = node:a;
