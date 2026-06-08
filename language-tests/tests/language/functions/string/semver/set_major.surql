/**
[test]

[[test.results]]
value = '"2.1.3"'

[[test.results]]
value = '"0.1.3"'

[[test.results]]
error = 'Failed to compute: "string::semver::set::major(-1)", as the operation results in a negative value.'

*/
string::semver::set::major("1.1.3", 2);
string::semver::set::major("1.1.3", 0);
string::semver::set::major("1.1.3", -1);
