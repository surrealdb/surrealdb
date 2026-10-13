/**
[env.capabilities]
allow-experimental = ["files"]

[test]
reason = "With the experimental target enabled the same calls run normally, so the gate does not cost the feature when it is on"

[[test.results]]
value = 'f"bucket:/key"'

[[test.results]]
value = "'bucket'"

[[test.results]]
value = "'/key'"

*/

RETURN type::file('bucket', 'key');
RETURN file::bucket(type::file('bucket', 'key'));
RETURN file::key(type::file('bucket', 'key'));
