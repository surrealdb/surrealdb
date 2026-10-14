/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "A custom function can delete a record and insert the same id again"
issue = 7322

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: t7322:1 }]"

[[test.results]]
value = "[{ id: t7322:1 }]"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: rel7322:1, in: in7322:1, out: out7322:1 }]"

[[test.results]]
value = "[{ id: rel7322:1, in: in7322:1, out: out7322:1 }]"
*/

DEFINE TABLE OVERWRITE t7322;

DEFINE FUNCTION OVERWRITE fn::re_add_record_7322() {
	DELETE t7322:1;
	INSERT INTO t7322 { id: 1 };
};

fn::re_add_record_7322();
fn::re_add_record_7322();

DEFINE TABLE OVERWRITE rel7322 TYPE RELATION;

DEFINE FUNCTION OVERWRITE fn::re_add_relation_7322() {
	DELETE rel7322:1;
	RELATE in7322:1 -> rel7322:1 -> out7322:1;
};

fn::re_add_relation_7322();
fn::re_add_relation_7322();
