/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism", "files"]

[test]
reason = "A DEFINE EXTENSION outside an import must state how its code is trusted: with neither SIGNER nor UNSIGNED it parses, and the executor refuses it."

[[test.results]]
error = "DEFINE EXTENSION needs SIGNER {…} or UNSIGNED"

*/

DEFINE EXTENSION ext::test FROM f"bucket:/module.wasm";
