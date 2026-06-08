/**
[test]

[[test.results]]
value = '"1.1.4"'

[[test.results]]
value = '"1.1.0"'

[[test.results]]
error = 'Failed to compute: "string::semver::set::patch(-1)", as the operation results in a negative value.'

*/
string::semver::set::patch("1.1.3", 4);
string::semver::set::patch("1.1.3", 0);
string::semver::set::patch("1.1.3", -1);
