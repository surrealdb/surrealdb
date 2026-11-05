/**
[test]
reason = "Tests string::semver::inc::patch incrementing the patch version"

[[test.results]]
value = "'1.2.4'"

[[test.results]]
value = "'0.0.1'"

[[test.results]]
value = "'1.2.4-beta'"

[[test.results]]
value = "'1.2.4+build.7'"

[[test.results]]
error = "Incorrect arguments for function string::semver::inc::patch(). Invalid semantic version"
*/

string::semver::inc::patch("1.2.3");
string::semver::inc::patch("0.0.0");
string::semver::inc::patch("1.2.3-beta");
string::semver::inc::patch("1.2.3+build.7");
string::semver::inc::patch("1.2.3.4");
