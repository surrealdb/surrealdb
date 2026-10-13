/**
[test]
reason = "Tests string::semver::minor extracting the minor version component"

[[test.results]]
value = "2"

[[test.results]]
value = "0"

[[test.results]]
value = "20"

[[test.results]]
value = "2"

[[test.results]]
error = "Incorrect arguments for function string::semver::minor(). Invalid semantic version"

[[test.results]]
error = "Incorrect arguments for function string::semver::minor(). Invalid semantic version"
*/

string::semver::minor("1.2.3");
string::semver::minor("0.0.0");
string::semver::minor("10.20.30");
string::semver::minor("1.2.3-alpha.1+build.5");
string::semver::minor("1.2");
string::semver::minor("not-semver");
