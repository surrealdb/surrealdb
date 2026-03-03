/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = "String literals containing colons inside function bodies must survive storage round-trips without being re-interpreted as record IDs by legacy strand parsing"

[[test.results]]
value = "NONE"

[[test.results]]
value = "'openai:gpt-5-mini'"
*/

DEFINE FUNCTION OVERWRITE fn::get_model() {
	RETURN 'openai:gpt-5-mini';
};

RETURN fn::get_model();
