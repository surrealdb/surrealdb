DEFINE MODULE mod::bla FROM f"abc" UNSIGNED;
DEFINE MODULE mod::bla FROM f"abc" UNSIGNED COMMENT "comment";
DEFINE MODULE mod::bla FROM f"abc" UNSIGNED PERMISSIONS FULL;
DEFINE MODULE mod::bla FROM f"abc" DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" UNSIGNED;
DEFINE MODULE mod::bla FROM f"abc" UNSIGNED CAPABILITIES {};
DEFINE MODULE mod::bla FROM f"abc" UNSIGNED CAPABILITIES { net: ["api.color.io:443"], functions: true, queries: true, scripting: true, write: true, memory: 67108864, timeout: 5s, strict_timeout: false };
DEFINE MODULE mod::bla FROM f"abc" SIGNER { key: "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIK0w release@acme" };
DEFINE MODULE mod::bla FROM silo::acme::color::<1.0.0> DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d" SIGNER { sigstore: { ca: ["Zm9vYmFy"], log: "bG9na2V5", tsa: ["dHNhY2VydA=="], issuer: "https://token.actions.githubusercontent.com", identity: "https://github.com/acme/color/.github/workflows/release.yml@refs/tags/v1.0.0", repository_id: "812345678", owner_id: "98765432" } } CAPABILITIES { functions: ["http::get"] } COMMENT "signed" PERMISSIONS NONE;
DEFINE MODULE mod::bla FROM f"abc" CAPABILITIES {} COMMENT "x" UNSIGNED DIGEST "sha256:5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d5d";
