/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """A silo package without DIGEST must be fetched and read to pin it. This build has no extension runtime and cannot read archives, so the definition fails and says to give DIGEST."""

[[test.results]]
error = "Cannot compute the extension's DIGEST: this build cannot read extension archives, so the definition must give `DIGEST`"

*/

DEFINE EXTENSION ext::color FROM silo::acme::color::<1.0.0> UNSIGNED;
