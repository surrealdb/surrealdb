/**
[test]
reason = "Tests string::semver::compare ordering of semantic version strings"

[[test.results]]
value = "0"

[[test.results]]
value = "-1"

[[test.results]]
value = "1"

[[test.results]]
value = "-1"

[[test.results]]
value = "-1"

[[test.results]]
value = "-1"

[[test.results]]
error = "Incorrect arguments for function string::semver::compare(). Invalid semantic version string for left argument"

[[test.results]]
error = "Incorrect arguments for function string::semver::compare(). Invalid semantic version string for right argument"
*/

string::semver::compare("1.2.3", "1.2.3");
string::semver::compare("1.2.3", "1.2.4");
string::semver::compare("2.0.0", "1.9.9");
string::semver::compare("1.0.0-alpha", "1.0.0");
string::semver::compare("1.0.0-alpha.1", "1.0.0-alpha.2");
string::semver::compare("1.0.0+build1", "1.0.0+build2");
string::semver::compare("1.2", "1.2.3");
string::semver::compare("1.2.3", "not-semver");
