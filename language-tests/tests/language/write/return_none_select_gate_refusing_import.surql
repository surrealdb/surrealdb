/**
[test]
reason = "Setup for return_none_select_gate_refusing.surql: tables whose select permission holds a construct the lowering decides at compile time (a stored function, a subquery, a full-text match), a view showing the stored rows of each, and the rows the subquery and the match read."
run = false
*/

DEFINE TABLE user SCHEMALESS;
CREATE user:alice;
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $id) )
	DURATION FOR TOKEN 1h, FOR SESSION 1h;

DEFINE FUNCTION fn::allowed($c: any) { RETURN $c > 0 } PERMISSIONS FULL;
DEFINE TABLE fn_gate SCHEMALESS
	PERMISSIONS FOR select WHERE fn::allowed(code) FOR create, update, delete FULL;
DEFINE TABLE fn_gate_state AS SELECT id, code, touched FROM fn_gate PERMISSIONS FULL;
CREATE fn_gate:1 SET code = 1;
CREATE fn_gate:2 SET code = -1;
CREATE fn_gate:3 SET code = 3;

DEFINE TABLE grant SCHEMALESS PERMISSIONS FULL;
CREATE grant:1 SET holder = user:alice, code = 1;
CREATE grant:2 SET holder = user:alice, code = 3;
DEFINE TABLE sub_gate SCHEMALESS
	PERMISSIONS FOR select WHERE (SELECT VALUE id FROM grant WHERE code = $parent.code AND holder = $auth.id) != []
	FOR create, update, delete FULL;
DEFINE TABLE sub_gate_state AS SELECT id, code, touched FROM sub_gate PERMISSIONS FULL;
CREATE sub_gate:1 SET code = 1;
CREATE sub_gate:2 SET code = 2;
CREATE sub_gate:3 SET code = 3;

DEFINE ANALYZER simple TOKENIZERS blank FILTERS lowercase;
DEFINE TABLE match_gate SCHEMALESS
	PERMISSIONS FOR select WHERE text @1@ 'alpha' FOR create, update, delete FULL;
DEFINE INDEX match_text ON match_gate FIELDS text FULLTEXT ANALYZER simple BM25;
DEFINE TABLE match_gate_state AS SELECT id, text, touched FROM match_gate PERMISSIONS FULL;
CREATE match_gate:1 SET text = 'alpha beta';
CREATE match_gate:2 SET text = 'gamma';
CREATE match_gate:3 SET text = 'alpha alpha';
