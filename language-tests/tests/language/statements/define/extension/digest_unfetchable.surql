/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = """Without DIGEST the definition must read its archive to pin it. This build has no extension runtime and cannot read archives, so the definition fails before it fetches anything, rather than store an extension that is pinned to nothing."""

[[test.results]]
error = "Cannot compute the extension's DIGEST: this build cannot read extension archives, so the definition must give `DIGEST`"

[[test.results]]
value = "{  }"

*/

DEFINE EXTENSION ext::color FROM f"nowhere:/color.surli" UNSIGNED;
(INFO FOR DB).extensions;
