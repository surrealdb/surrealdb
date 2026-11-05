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
value = "[{ id: user:alice }, { id: user:bob }, { id: user:carol }]"

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
value = "[{ id: doc:1, txt: 'one' }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: gated:1 }, { id: gated:2 }, { id: gated:3 }]"

[[test.results]]
value = "[{ id: mixed:1 }, { id: mixed:2 }, { id: mixed:3 }]"

*/
DEFINE ACCESS user ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $user) )
	DURATION FOR TOKEN 18m, FOR SESSION 12h;
DEFINE ACCESS other ON DATABASE TYPE RECORD
	SIGNIN ( SELECT * FROM type::record('user', $user) )
	DURATION FOR TOKEN 18m, FOR SESSION 12h;

DEFINE TABLE user SCHEMALESS PERMISSIONS FULL;
DEFINE TABLE doc SCHEMALESS PERMISSIONS FULL;

CREATE user:alice, user:bob, user:carol;

-- Each guard and filter below reads `$access` in a conjunct that answers the same at every row,
-- over a table of more than one row.
DEFINE FUNCTION fn::low($s: string) -> string { RETURN string::lowercase($s) }
	PERMISSIONS WHERE count(SELECT * FROM user WHERE $access = 'user') > 0;
DEFINE FUNCTION fn::other() -> string { RETURN 'other' }
	PERMISSIONS WHERE count(SELECT * FROM user WHERE $access = 'other') > 0;
DEFINE FUNCTION fn::seen() { RETURN SELECT VALUE id FROM user WHERE $access = 'user' };

DEFINE ANALYZER low FUNCTION fn::low TOKENIZERS blank;
DEFINE INDEX doc_low ON doc FIELDS txt FULLTEXT ANALYZER low BM25;
CREATE doc:1 SET txt = 'one';

-- A table's select permission whose conjunct on `$access` answers the same at every row.
DEFINE TABLE gated SCHEMALESS PERMISSIONS FOR select WHERE $access = 'user';
DEFINE TABLE mixed SCHEMALESS PERMISSIONS FOR select WHERE $access = 'user' AND id != NONE;
CREATE gated:1, gated:2, gated:3;
CREATE mixed:1, mixed:2, mixed:3;
