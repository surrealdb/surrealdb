/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[test]
reason = """
A string parameter holding colons and a dash is stored whole. A record id read
from a string is the whole string or an error: `value:focus:outline-none` is
refused rather than read as `value:focus`, and a key holding `:` and `-` is
written escaped or passed to type::record as its own argument.
"""
issue = 5749

[[test.results]]
value = "NONE"

[[test.results]]
value = "[{ id: value:`focus:outline-none`, value: 'focus:outline-none' }]"

[[test.results]]
value = "['focus:outline-none']"

[[test.results]]
error = "Could not cast into `record` using input `'value:focus:outline-none'`"

[[test.results]]
error = "Could not cast into `record` using input `'value:focus:outline-none'`"

[[test.results]]
value = "false"

[[test.results]]
value = "true"

[[test.results]]
value = "'focus:outline-none'"

[[test.results]]
value = "value:focus"
*/
LET $data = "focus:outline-none";
UPSERT type::record("value", "focus:outline-none") SET value = $data;
SELECT VALUE value FROM value;
RETURN type::record("value:focus:outline-none");
RETURN <record> "value:focus:outline-none";
RETURN string::is_record("value:focus:outline-none");
RETURN type::record("value:⟨focus:outline-none⟩") == type::record("value", "focus:outline-none");
RETURN record::id(type::record("value:`focus:outline-none`"));
RETURN type::record("value:focus");
