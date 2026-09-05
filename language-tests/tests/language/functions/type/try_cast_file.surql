/**
[test]
wip = true
reason = "Ensure that type::try_file function actually does what it needs to"

# try_file
[[test.results]] # 0
error = "Incorrect arguments for function type::try_file(). Expected 2 arguments"
[[test.results]] # 1
value = "NONE"
[[test.results]] # 2
value = "f\"2:/2\""
[[test.results]] # 3
error = "Incorrect arguments for function type::try_file(). Expected 2 arguments"
*/

RETURN type::try_file(2);
RETURN type::try_file(2, 2);
RETURN type::try_file('2', '2');
RETURN type::try_file('2', '2', 'NE');
