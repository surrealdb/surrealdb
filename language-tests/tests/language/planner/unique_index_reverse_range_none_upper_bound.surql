/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "Unique index range with NONE or NULL upper bound plus ORDER BY DESC (reverse scan) must return correct counts vs full table (guards compute_unique_range_end_key NONE/NULL + backward iterator)"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: u:a, v: 10 }, { id: u:b }, { id: u:c, v: 20 }, { id: u:d }]"

[[test.results]]
value = "[{ id: u:b }, { id: u:d }]"

[[test.results]]
value = "[{ id: u:d }, { id: u:b }]"

[[test.results]]
value = "[]"

[[test.results]]
value = "'OK'"

[[test.results]]
value = "[{ id: w:a, v: 10 }, { id: w:b }, { id: w:c, v: NULL }, { id: w:d }, { id: w:e, v: NULL }]"

[[test.results]]
value = "[{ id: w:b }, { id: w:d }, { id: w:c, v: NULL }, { id: w:e, v: NULL }]"

[[test.results]]
value = "[{ id: w:e, v: NULL }, { id: w:c, v: NULL }, { id: w:d }, { id: w:b }]"

[[test.results]]
value = "[{ id: w:b }, { id: w:d }]"

[[test.results]]
value = "[{ id: w:d }, { id: w:b }]"

[[test.results]]
value = "[{ id: u:a, v: 10 }, { id: u:d }, { id: u:b }]"

[[test.results]]
value = "[{ id: u:c, v: 20 }, { id: u:a, v: 10 }, { id: u:d }, { id: u:b }]"

*/

{
	DEFINE TABLE OVERWRITE u SCHEMAFULL;
	DEFINE FIELD OVERWRITE v ON u TYPE option<int>;
	DEFINE INDEX OVERWRITE u_v ON u FIELDS v UNIQUE;
	CREATE u:a SET v = 10;
	CREATE u:b SET v = NONE;
	CREATE u:c SET v = 20;
	CREATE u:d SET v = NONE;
	RETURN "OK";
};

RETURN SELECT * FROM u;
-- Inclusive NONE upper: must include NONE-valued rows; ORDER BY v DESC uses reverse index path
RETURN SELECT * FROM u WHERE v <= NONE ORDER BY v ASC;
RETURN SELECT * FROM u WHERE v <= NONE ORDER BY v DESC;
RETURN SELECT * FROM u WHERE v < NONE ORDER BY v DESC;

-- NULL sentinel on schemaless (SCHEMAFULL option<int> rejects NULL)
{
	DEFINE TABLE OVERWRITE w SCHEMALESS;
	DEFINE INDEX OVERWRITE w_v ON w FIELDS v UNIQUE;
	CREATE w:a SET v = 10;
	CREATE w:b SET v = NONE;
	CREATE w:c SET v = NULL;
	CREATE w:d SET v = NONE;
	CREATE w:e SET v = NULL;
	RETURN "OK";
};

RETURN SELECT * FROM w;
RETURN SELECT * FROM w WHERE v <= NULL ORDER BY v ASC;
RETURN SELECT * FROM w WHERE v <= NULL ORDER BY v DESC;
RETURN SELECT * FROM w WHERE v < NULL ORDER BY v ASC;
RETURN SELECT * FROM w WHERE v < NULL ORDER BY v DESC;

-- Non-NULL inclusive upper bound: boundary row must not be dropped in DESC scan
RETURN SELECT * FROM u WHERE v <= 10 ORDER BY v DESC;
RETURN SELECT * FROM u WHERE v <= 20 ORDER BY v DESC;
