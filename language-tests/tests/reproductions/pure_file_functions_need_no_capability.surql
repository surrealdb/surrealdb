/**
[test]
reason = "type::file, file::bucket and file::key run with no capability enabled"

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
