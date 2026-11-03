/**
[env]
namespace = true
database = true
auth = { level = "owner" }

[env.capabilities]
allow-experimental = ["surrealism"]

[test]
reason = "The `MODULE` keyword and the `mod::` prefix stay accepted during 4.0 as deprecated aliases of `EXTENSION` and `ext::`, in any combination. Each spelling defines the same definition under the same catalog key, which every render writes as DEFINE EXTENSION and `ext::`. INFO FOR DB lists them under `extensions`, keyed by `ext::` name, and has no `modules` key. Using an alias logs a deprecation warning; the statement's result is unchanged."

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
value = "NONE"

[[test.results]]
error = "The extension 'ext::a' already exists"

[[test.results]]
value = '''{ "ext::a": "DEFINE EXTENSION ext::a FROM f\"bucket:/a.wasm\" DIGEST 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d' UNSIGNED CAPABILITIES {} PERMISSIONS FULL", "ext::b": "DEFINE EXTENSION ext::b FROM f\"bucket:/b.wasm\" DIGEST 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d' UNSIGNED CAPABILITIES {} PERMISSIONS FULL", "ext::c": "DEFINE EXTENSION ext::c FROM silo::acme::c<0.1.0> DIGEST 'sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d' UNSIGNED CAPABILITIES {} PERMISSIONS FULL" }'''

[[test.results]]
value = "NONE"
*/

-- 0: both aliases
DEFINE MODULE mod::a FROM f"bucket:/a.wasm" DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" UNSIGNED;

-- 1: the alias keyword with the new prefix
DEFINE MODULE ext::b FROM f"bucket:/b.wasm" DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" UNSIGNED;

-- 2: the new keyword with the alias prefix
DEFINE EXTENSION mod::c FROM silo::acme::c::<0.1.0> DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" UNSIGNED;

-- 3: a name defined through one spelling already exists in the other
DEFINE EXTENSION ext::a FROM f"bucket:/a.wasm" DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" UNSIGNED;

-- 4: every definition renders in the new spelling
(INFO FOR DB).extensions;

-- 5: INFO FOR DB lists extensions under `extensions` only; it has no `modules` key
(INFO FOR DB).modules;
