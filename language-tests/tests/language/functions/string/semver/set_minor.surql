/**
[test]

[[test.results]]
value = '"1.2.3"'

[[test.results]]
value = '"1.0.3"'

[[test.results]]
error = 'Failed to compute: "string::semver::set::minor(-1)", as the operation results in a negative value.'

*/
string::semver::set::minor("1.1.3", 2);
string::semver::set::minor("1.1.3", 0);
string::semver::set::minor("1.1.3", -1);
