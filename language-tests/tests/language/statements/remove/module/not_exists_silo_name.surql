/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = "The silo `organisation::package<major.minor.patch>` module name form reaches the catalog and is echoed verbatim in the not-found error. Kept apart from the `mod::` cases in not_exists.surql because the greenfield parser does not yet accept the version suffix, so this file is listed in that parser's ignore set."

[[test.results]]
error = "The module 'silo::acme::pkg::<1.0.0>' does not exist"

*/

REMOVE MODULE silo::acme::pkg::<1.0.0>;
