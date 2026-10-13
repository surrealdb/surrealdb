/**
[test]
reason = "Tests string::semver::patch extracting the patch version component"

[[test.results]]
value = "3"

[[test.results]]
value = "0"

[[test.results]]
value = "30"

[[test.results]]
value = "3"

[[test.results]]
error = "Incorrect arguments for function string::semver::patch(). Invalid semantic version"

[[test.results]]
error = "Incorrect arguments for function string::semver::patch(). Invalid semantic version"
*/

string::semver::patch("1.2.3");
string::semver::patch("0.0.0");
string::semver::patch("10.20.30");
string::semver::patch("1.2.3-alpha.1+build.5");
string::semver::patch("1.2.3.4");
string::semver::patch("");
