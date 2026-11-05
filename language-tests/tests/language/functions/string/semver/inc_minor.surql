/**
[test]
reason = "Tests string::semver::inc::minor incrementing the minor version and resetting patch"

[[test.results]]
value = "'1.3.0'"

[[test.results]]
value = "'0.1.0'"

[[test.results]]
value = "'1.3.0-beta'"

[[test.results]]
value = "'1.3.0+build.7'"

[[test.results]]
error = "Incorrect arguments for function string::semver::inc::minor(). Invalid semantic version"
*/

string::semver::inc::minor("1.2.3");
string::semver::inc::minor("0.0.0");
string::semver::inc::minor("1.2.3-beta");
string::semver::inc::minor("1.2.3+build.7");
string::semver::inc::minor("not-semver");
