/**
[test]
reason = "Tests string::semver::inc::major incrementing the major version and resetting minor and patch"

[[test.results]]
value = "'2.0.0'"

[[test.results]]
value = "'1.0.0'"

[[test.results]]
value = "'2.0.0-alpha.1'"

[[test.results]]
value = "'2.0.0+build.7'"

[[test.results]]
error = "Incorrect arguments for function string::semver::inc::major(). Invalid semantic version"
*/

string::semver::inc::major("1.2.3");
string::semver::inc::major("0.0.1");
string::semver::inc::major("1.2.3-alpha.1");
string::semver::inc::major("1.2.3+build.7");
string::semver::inc::major("1.2");
