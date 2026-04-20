/**
[env]
namespace = true
database = true
auth = { level = "owner" }
planner-strategy = ["all-ro", "best-effort-ro"]

[test]
reason = "DEFINE FUNCTION: LET inside IF inside FOR must not drop planned LET bindings when a later statement uses legacy compute (CREATE) (#7131)"
issue = 7131

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
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ count: 1 }]"

*/

DEFINE TABLE t7131_topic SCHEMAFULL;
DEFINE FIELD name ON t7131_topic TYPE string ASSERT string::trim($value) != "";
DEFINE INDEX t7131_name ON t7131_topic FIELDS name UNIQUE;

DEFINE TABLE t7131_cnt SCHEMAFULL;

DEFINE FUNCTION fn::t7131::rid($table: string, $id: record) {
	RETURN type::record($table, record::id($id));
};

DEFINE FUNCTION fn::t7131::repro() {
	FOR $t IN ['cats'] {
		IF true {
			LET $topic_id = CREATE ONLY t7131_topic SET name = $t RETURN VALUE id;
			LET $cnt_id = fn::t7131::rid('t7131_cnt', $topic_id);
			CREATE ONLY $cnt_id RETURN NONE;
		};
	};
};

fn::t7131::repro();

SELECT count() FROM t7131_topic GROUP ALL;
