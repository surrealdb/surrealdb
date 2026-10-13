/**
[test]
reason = "Tests string::semver::major extracting the major version component"

[[test.results]]
value = "1"

[[test.results]]
value = "0"

[[test.results]]
value = "10"

[[test.results]]
value = "1"

[[test.results]]
error = "Incorrect arguments for function string::semver::major(). Invalid semantic version"

[[test.results]]
error = "Incorrect arguments for function string::semver::major(). Invalid semantic version"
*/

string::semver::major("1.2.3");
string::semver::major("0.0.0");
string::semver::major("10.20.30");
string::semver::major("1.2.3-alpha.1+build.5");
string::semver::major("v1.2.3");
string::semver::major("");
